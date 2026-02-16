package p2p

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"github.com/dhruv-ship-it/go-bittorrent/client"
	"github.com/dhruv-ship-it/go-bittorrent/message"
	"github.com/dhruv-ship-it/go-bittorrent/peers"
	"log"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"
)

// MaxBlockSize is the maximum size of a block
const MaxBlockSize = 16384

// MaxBacklog is the maximum number of blocks we will queue up
const MaxBacklog = 5

// Torrent holds data required to download a torrent from a list of peers
type Torrent struct {
	Peers       []peers.Peer
	PeerID      [20]byte
	InfoHash    [20]byte
	PieceHashes [][20]byte
	PieceLength int
	Length      int
	Name        string
}

type pieceWork struct {
	index  int
	hash   [20]byte
	length int
}

type pieceResult struct {
	index int
	buf   []byte
}

type pieceProgress struct {
	index      int
	client     *client.Client
	buf        []byte
	downloaded int
	requested  int
	backlog    int
}

func (state *pieceProgress) readMessage() error {
	msg, err := state.client.Read()
	if err != nil {
		return err
	}
	if msg == nil {
		return nil
	}
	switch msg.ID {
	case message.MsgUnchoke:
		state.client.Chocked = false
	case message.MsgChoke:
		state.client.Chocked = true
	case message.MsgHave:
		index, err := message.ParseHave(msg)
		if err != nil {
			return err
		}
		state.client.Bittfield.SetPiece(index)
	case message.MsgPiece:
		n, err := message.ParsePiece(state.index, state.buf, msg)
		if err != nil {
			return err
		}
		state.downloaded += n
		state.backlog--
	}
	return nil
}

func attemptDownloadPiece(c *client.Client, pw *pieceWork) ([]byte, error) {
	state := pieceProgress{
		index:  pw.index,
		client: c,
		buf:    make([]byte, pw.length),
	}
	// Setting a deadline helps get unresponsive peers unstuck.
	// 30 seconds is more than enough time to download a 262 KB piece.
	c.Conn.SetDeadline(time.Now().Add(30 * time.Second))
	defer c.Conn.SetDeadline(time.Time{})

	for state.downloaded < pw.length {
		// if unchoncked send request until we have enough unfufilled requests
		if !state.client.Chocked {
			for state.backlog < MaxBacklog && state.requested < pw.length {
				blockSize := MaxBlockSize
				// Last block might be shorter than the typical block.
				if pw.length-state.requested < MaxBlockSize {
					blockSize = pw.length - state.requested
				}
				err := c.SendRequest(pw.index, state.requested, blockSize)
				if err != nil {
					return nil, err
				}
				state.requested += blockSize
				state.backlog++
			}
		}
		err := state.readMessage()
		if err != nil {
			return nil, err
		}

	}
	return state.buf, nil
}

func checkIntegrity(pw *pieceWork, buf []byte) error {
	hash := sha1.Sum(buf)
	if !bytes.Equal(hash[:], pw.hash[:]) {
		return fmt.Errorf("index %d failed integrity check", pw.index)
	}
	return nil
}

func (t *Torrent) startDownloadWorker(c *client.Client, workQueue chan *pieceWork, result chan *pieceResult) {
	defer c.Conn.Close()
	
	for pw := range workQueue {
		if !c.Bittfield.HasPiece(pw.index) {
			workQueue <- pw // put piece back on the queue
			continue
		}
		// download piece
		buf, err := attemptDownloadPiece(c, pw)
		if err != nil {
			log.Printf("exiting: %s", err)
			workQueue <- pw // put piece back on the queue
			return
		}

		err = checkIntegrity(pw, buf)
		if err != nil {
			log.Printf("Piece #%d failed integrity check: %s", pw.index, err)
			workQueue <- pw // put piece back on the queue
			continue
		}

		c.SendHave(pw.index)
		result <- &pieceResult{pw.index, buf}
	}
}

func (t *Torrent) calculateBoundsForPiece(index int) (begin int, end int) {
	begin = index * t.PieceLength
	end = begin + t.PieceLength
	if end >= t.Length {
		end = t.Length
	}
	return begin, end
}

func (t *Torrent) calculatePieceSize(index int) int {
	begin, end := t.calculateBoundsForPiece(index)
	return end - begin
}

// Download downloads the torrent and streams pieces directly to the provided file
func (t *Torrent) Download(outfile *os.File) error {
	log.Println("Starting download for", t.Name)
	
	// Step 1: Initialize clients once (no double handshake)
	var clients []*client.Client
	for _, peer := range t.Peers {
		c, err := client.New(peer, t.PeerID, t.InfoHash)
		if err != nil {
			log.Printf("Failed handshake with %s: %v", peer.String(), err)
			continue
		}
		
		// Send interested and unchoke to get bitfield
		c.SendUnchoke()
		c.SendInterested()
		
		log.Printf("Completed handshake with %s", peer.String())
		clients = append(clients, c)
	}
	
	// Check if we have any successful connections
	if len(clients) == 0 {
		return fmt.Errorf("no successful peer connections")
	}
	
	log.Printf("Connected to %d peers successfully", len(clients))
	
	// Step 2: Compute piece availability using live clients
	availability := make([]int, len(t.PieceHashes))
	for _, c := range clients {
		for pieceIndex := 0; pieceIndex < len(t.PieceHashes); pieceIndex++ {
			if c.Bittfield.HasPiece(pieceIndex) {
				availability[pieceIndex]++
			}
		}
	}
	
	// Log availability for debugging
	log.Printf("Piece availability computed:")
	for i, count := range availability {
		if count > 0 {
			log.Printf("Piece #%d: %d peers", i, count)
		}
	}
	
	// Step 3: Create sorted piece indexes (rarest first)
	indexes := make([]int, len(t.PieceHashes))
	for i := range indexes {
		indexes[i] = i
	}
	
	// Sort by availability (ascending = rarest first)
	sort.Slice(indexes, func(i, j int) bool {
		return availability[indexes[i]] < availability[indexes[j]]
	})
	
	// Step 4: Push work in rarest-first order
	workQueue := make(chan *pieceWork, len(t.PieceHashes))
	resultQueue := make(chan *pieceResult)
	for _, index := range indexes {
		hash := t.PieceHashes[index]
		length := t.calculatePieceSize(index)
		workQueue <- &pieceWork{index, hash, length}
	}

	// Start workers using pre-initialized clients
	for _, c := range clients {
		go t.startDownloadWorker(c, workQueue, resultQueue)
	}

	// Preallocate file to full torrent size
	err := outfile.Truncate(int64(t.Length))
	if err != nil {
		return fmt.Errorf("failed to preallocate file: %w", err)
	}

	// Track completion using a map for thread safety
	completed := make(map[int]bool)
	completedMutex := sync.RWMutex{}
	donePieces := 0

	for donePieces < len(t.PieceHashes) {
		res := <-resultQueue
		
		// Verify piece before writing (integrity check preserved)
		err = checkIntegrity(&pieceWork{index: res.index, hash: t.PieceHashes[res.index], length: len(res.buf)}, res.buf)
		if err != nil {
			log.Printf("Piece #%d failed integrity check: %s", res.index, err)
			workQueue <- &pieceWork{res.index, t.PieceHashes[res.index], t.calculatePieceSize(res.index)} // retry piece
			continue
		}

		// Thread-safe check if piece already written
		completedMutex.RLock()
		if completed[res.index] {
			completedMutex.RUnlock()
			continue // already written by another worker
		}
		completedMutex.RUnlock()

		// Write piece directly to file at correct offset
		offset := int64(res.index) * int64(t.PieceLength)
		_, err = outfile.WriteAt(res.buf, offset)
		if err != nil {
			return fmt.Errorf("failed to write piece #%d at offset %d: %w", res.index, offset, err)
		}

		// Mark piece as completed
		completedMutex.Lock()
		completed[res.index] = true
		completedMutex.Unlock()
		
		donePieces++

		percent := float64(donePieces) / float64(len(t.PieceHashes)) * 100
		numWorkers := runtime.NumGoroutine() - 1 // subtract main thread

		log.Printf("(%0.2f%%) Downloaded piece #%d from %d peers", percent, res.index, numWorkers)
	}
	close(workQueue)
	return nil
}
