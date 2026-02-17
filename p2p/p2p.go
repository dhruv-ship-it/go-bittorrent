package p2p

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"github.com/dhruv-ship-it/go-bittorrent/bitfield"
	"github.com/dhruv-ship-it/go-bittorrent/client"
	"github.com/dhruv-ship-it/go-bittorrent/message"
	"github.com/dhruv-ship-it/go-bittorrent/peers"
	"log"
	"os"
	"runtime"
	"sync"
	"time"
)

// scheduler manages dynamic piece assignment with rarest-first selection
// 
// Lifecycle:
// 1. Peer connects → addPeer() tracks all pieces + sets initial availability
// 2. Peer sends HAVE → addPieceToPeer() updates inventory + increments availability (if new)
// 3. Peer disconnects → removePeer() decrements availability for all tracked pieces
// 4. Workers request pieces → getNextPiece() selects rarest available piece
// 5. Piece completes → markCompleted() updates completion state
//
// This ensures perfect accounting between global availability[] and per-peer peerPieces[]
type scheduler struct {
	availability   []int
	inProgress     map[int]bool
	completed      map[int]bool
	peerPieces     map[string]map[int]bool // peer.String() → piece index → hasPiece
	mu             sync.Mutex
}

// decrementAvailability reduces piece availability when peer disconnects
// NOTE: This function is no longer needed - availability decrements are handled
// by removePeer() which maintains perfect accounting with peerPieces map

// addPieceToPeer adds a piece to a peer's inventory and increments availability if new
func (s *scheduler) addPieceToPeer(peerStr string, index int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pieces, exists := s.peerPieces[peerStr]
	if !exists {
		return
	}

	// Only increment if peer did not previously have this piece
	if !pieces[index] {
		pieces[index] = true
		s.availability[index]++
	}
}

// addPeer tracks a new peer and their available pieces
func (s *scheduler) addPeer(peerStr string, bitfield bitfield.Bitfield) {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	s.peerPieces[peerStr] = make(map[int]bool)
	for pieceIndex := 0; pieceIndex < len(s.availability); pieceIndex++ {
		if bitfield.HasPiece(pieceIndex) {
			s.peerPieces[peerStr][pieceIndex] = true
		}
	}
}

// removePeer decrements availability for all pieces a peer had
func (s *scheduler) removePeer(peerStr string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if pieces, exists := s.peerPieces[peerStr]; exists {
		for pieceIndex := range pieces {
			if s.availability[pieceIndex] > 0 {
				s.availability[pieceIndex]--
			}
		}
		delete(s.peerPieces, peerStr)
	}
}

// getNextPiece selects the rarest available piece dynamically
// 
// Linear scan is used instead of a priority queue because:
// 1. Simplicity: Easier to reason about and debug
// 2. Low overhead: For typical torrent sizes (thousands of pieces), scan cost is negligible
// 3. Thread safety: Single lock acquisition ensures consistent state
// 4. Dynamic updates: Availability changes frequently, making complex data structures less beneficial
// 5. Educational value: Demonstrates clear algorithmic thinking without optimization complexity
func (s *scheduler) getNextPiece() *pieceWork {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	// Find piece with minimum availability
	bestPiece := -1
	bestAvailability := int(^uint(0) >> 1) // max int
	
	for index := 0; index < len(s.availability); index++ {
		if s.inProgress[index] || s.completed[index] || s.availability[index] == 0 {
			continue
		}
		
		if s.availability[index] < bestAvailability {
			bestAvailability = s.availability[index]
			bestPiece = index
		}
	}
	
	if bestPiece == -1 {
		return nil // No pieces available
	}
	
	// Mark as in progress
	s.inProgress[bestPiece] = true
	
	return &pieceWork{
		index:  bestPiece,
		hash:   [20]byte{}, // Will be filled by caller
		length: 0,          // Will be filled by caller
	}
}

// completedCount returns the number of completed pieces (thread-safe)
func (s *scheduler) completedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.completed)
}

// markCompleted marks a piece as successfully downloaded
func (s *scheduler) markCompleted(index int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	s.completed[index] = true
	s.inProgress[index] = false
}

// markFailed marks a piece as failed (available for retry)
func (s *scheduler) markFailed(index int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	s.inProgress[index] = false
}

// incrementAvailability updates piece availability when HAVE message received
func (s *scheduler) incrementAvailability(index int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	s.availability[index]++
}

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
	sched      *scheduler
	peerStr    string
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
		if state.sched != nil {
			state.sched.addPieceToPeer(state.peerStr, index)
		}
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

func attemptDownloadPiece(c *client.Client, pw *pieceWork, sched *scheduler, peerStr string) ([]byte, error) {
	state := pieceProgress{
		index:  pw.index,
		client: c,
		buf:    make([]byte, pw.length),
		sched:  sched,
		peerStr: peerStr,
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

func (t *Torrent) startDownloadWorker(c *client.Client, sched *scheduler, result chan *pieceResult) {
	// Track peer for cleanup on exit
	peerStr := c.Conn.RemoteAddr().String()
	defer func() {
		// Remove peer from availability tracking when connection closes
		sched.removePeer(peerStr)
		c.Conn.Close()
	}()
	
	for {
		pw := sched.getNextPiece()
		if pw == nil {
			if sched.completedCount() == len(t.PieceHashes) {
				return // All pieces completed
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}
		
		// Fill in piece details
		pw.hash = t.PieceHashes[pw.index]
		pw.length = t.calculatePieceSize(pw.index)
		
		if !c.Bittfield.HasPiece(pw.index) {
			sched.markFailed(pw.index)
			continue
		}
		
		// download piece
		buf, err := attemptDownloadPiece(c, pw, sched, peerStr)
		if err != nil {
			log.Printf("exiting: %s", err)
			sched.markFailed(pw.index)
			continue
		}

		err = checkIntegrity(pw, buf)
		if err != nil {
			log.Printf("Piece #%d failed integrity check: %s", pw.index, err)
			sched.markFailed(pw.index)
			continue
		}

		c.SendHave(pw.index)
		result <- &pieceResult{pw.index, buf}
		sched.markCompleted(pw.index)
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
	
	// Step 2: Initialize dynamic scheduler
	sched := &scheduler{
		availability:   make([]int, len(t.PieceHashes)),
		inProgress:     make(map[int]bool),
		completed:      make(map[int]bool),
		peerPieces:     make(map[string]map[int]bool),
	}
	
	// Compute initial availability from connected clients and track peer pieces
	for _, c := range clients {
		// Add peer to scheduler tracking
		sched.addPeer(c.Conn.RemoteAddr().String(), c.Bittfield)
		
		// Compute availability
		for pieceIndex := 0; pieceIndex < len(t.PieceHashes); pieceIndex++ {
			if c.Bittfield.HasPiece(pieceIndex) {
				sched.availability[pieceIndex]++
			}
		}
	}
	
	// Initialize all pieces as available (no pending map needed)
	// getNextPiece will handle piece selection logic
	
	// Log availability for debugging
	log.Printf("Piece availability computed:")
	for i, count := range sched.availability {
		if count > 0 {
			log.Printf("Piece #%d: %d peers", i, count)
		}
	}
	
	// Step 3: Start workers with dynamic scheduler
	resultQueue := make(chan *pieceResult)
	for _, c := range clients {
		go t.startDownloadWorker(c, sched, resultQueue)
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

	// Step 4: Process results until all pieces completed
	for sched.completedCount() < len(t.PieceHashes) {
		res := <-resultQueue
		
		// Verify piece before writing (integrity check preserved)
		err = checkIntegrity(&pieceWork{index: res.index, hash: t.PieceHashes[res.index], length: len(res.buf)}, res.buf)
		if err != nil {
			log.Printf("Piece #%d failed integrity check: %s", res.index, err)
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
	
	return nil
}
