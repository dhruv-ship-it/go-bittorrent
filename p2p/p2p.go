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
// 1. Peer connects → addPeer() tracks pieces + increments availability (single source)
// 2. Peer sends HAVE → addPieceToPeer() updates inventory + increments availability (if new)
// 3. Peer disconnects → removePeer() decrements availability for all tracked pieces
// 4. Workers request pieces → getNextPiece() selects rarest available piece (event-driven)
// 5. Piece completes → markCompleted() updates state + broadcasts to waiting workers
//
// This ensures single source of truth and event-driven coordination
type scheduler struct {
	availability   []int
	inProgress     map[int]bool
	completed      map[int]bool
	peerPieces     map[string]map[int]bool // peer.String() → piece index → hasPiece
	mu           sync.Mutex
	cond         *sync.Cond
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
		
		// Wake up workers waiting for pieces
		s.cond.Broadcast()
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
			s.availability[pieceIndex]++ // Single source of availability increment
		}
	}
	
	// Wake up workers waiting for pieces
	s.cond.Broadcast()
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
	
	// Wake up workers waiting for pieces
	s.cond.Broadcast()
}

// getNextPiece selects the rarest available piece dynamically (event-driven)
// 
// Linear scan is used instead of a priority queue because:
// 1. Simplicity: Easier to reason about and debug
// 2. Low overhead: For typical torrent sizes (thousands of pieces), scan cost is negligible
// 3. Thread safety: Single lock acquisition ensures consistent state
// 4. Dynamic updates: Availability changes frequently, making complex data structures less beneficial
// 5. Educational value: Demonstrates clear algorithmic thinking without optimization complexity
func (s *scheduler) getNextPiece(totalPieces int) *pieceWork {
	s.mu.Lock()
	for {
		// If all pieces completed → exit
		if len(s.completed) == totalPieces {
			s.mu.Unlock()
			return nil
		}
		
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
		
		if bestPiece != -1 {
			// Mark as in progress and return
			s.inProgress[bestPiece] = true
			s.mu.Unlock()
			return &pieceWork{
				index:  bestPiece,
				hash:   [20]byte{}, // Will be filled by caller
				length: 0,          // Will be filled by caller
			}
		}
		
		// Correct usage: wait WHILE holding lock
		s.cond.Wait()
	}
}

// completedCount returns the number of completed pieces (thread-safe)
func (s *scheduler) completedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.completed)
}

// isCompleted checks if a piece is already completed (thread-safe)
func (s *scheduler) isCompleted(index int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.completed[index]
}

// markCompleted marks a piece as successfully downloaded
func (s *scheduler) markCompleted(index int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	s.completed[index] = true
	s.inProgress[index] = false
	
	// Wake up workers waiting for pieces
	s.cond.Broadcast()
}

// markFailed marks a piece as failed (available for retry)
func (s *scheduler) markFailed(index int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	s.inProgress[index] = false
	
	// Wake up workers waiting for pieces
	s.cond.Broadcast()
}

// incrementAvailability updates piece availability when HAVE message received
func (s *scheduler) incrementAvailability(index int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	s.availability[index]++
}

// pieceBufferPool reuses piece buffers to reduce memory allocations
var pieceBufferPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 0)
	},
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
	// Get buffer from pool, resize if needed
	raw := pieceBufferPool.Get().([]byte)
	if cap(raw) < pw.length {
		raw = make([]byte, pw.length)
	}
	buf := raw[:pw.length]
	
	state := pieceProgress{
		index:  pw.index,
		client: c,
		buf:    buf,
		sched:  sched,
		peerStr: peerStr,
	}
	// Setting a deadline helps get unresponsive peers unstuck.
	// 30 seconds is more than enough time to download a 262 KB piece.
	c.Conn.SetDeadline(time.Now().Add(30 * time.Second))
	defer func() {
		c.Conn.SetDeadline(time.Time{})
		// Note: Buffer returned to pool ONLY in Download() after successful write
	}()
	
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
			// On error, return buffer to pool to prevent leak
			pieceBufferPool.Put(buf[:0])
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

func (t *Torrent) startDownloadWorker(c *client.Client, sched *scheduler, result chan *pieceResult, workerDone chan struct{}) {
	// Track peer for cleanup on exit
	peerStr := c.Conn.RemoteAddr().String()
	defer func() {
		// Remove peer from availability tracking when connection closes
		sched.removePeer(peerStr)
		c.Conn.Close()
		// Signal worker completion
		workerDone <- struct{}{}
	}()
	
	for {
		pw := sched.getNextPiece(len(t.PieceHashes))
		if pw == nil {
			return // All pieces completed
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
		// Note: Completion marked in Download() after successful disk write
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
	
	// Performance instrumentation
	var peakHeap uint64
	maxActiveWorkers := 0
	
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
	sched.cond = sync.NewCond(&sched.mu)
	
	// Compute initial availability from connected clients and track peer pieces
	for _, c := range clients {
		// Add peer to scheduler tracking (handles availability incrementing)
		sched.addPeer(c.Conn.RemoteAddr().String(), c.Bittfield)
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
	workerDone := make(chan struct{}, len(clients))
	for _, c := range clients {
		go t.startDownloadWorker(c, sched, resultQueue, workerDone)
	}
	
	// Track maximum concurrent workers
	activeWorkers := len(clients)
	maxActiveWorkers = activeWorkers

	// Preallocate file to full torrent size
	err := outfile.Truncate(int64(t.Length))
	if err != nil {
		return fmt.Errorf("failed to preallocate file: %w", err)
	}
	
	donePieces := 0

	// Step 4: Process results until all pieces completed
	activeWorkers = len(clients)
	for sched.completedCount() < len(t.PieceHashes) {
		select {
		case res := <-resultQueue:
			// Verify piece before writing (integrity check preserved)
			err = checkIntegrity(&pieceWork{index: res.index, hash: t.PieceHashes[res.index], length: len(res.buf)}, res.buf)
			if err != nil {
				log.Printf("Piece #%d failed integrity check: %s", res.index, err)
				continue
			}

			// Thread-safe check if piece already written
			if sched.isCompleted(res.index) {
				continue // already written by another worker
			}

			// Write piece directly to file at correct offset
			offset := int64(res.index) * int64(t.PieceLength)
			_, err = outfile.WriteAt(res.buf, offset)
			if err != nil {
				return fmt.Errorf("failed to write piece #%d at offset %d: %w", res.index, offset, err)
			}
			
			// Return buffer to pool for reuse
			pieceBufferPool.Put(res.buf[:0])
			
			// Track peak heap usage
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			if m.HeapAlloc > peakHeap {
				peakHeap = m.HeapAlloc
			}
			
			// Mark piece as completed
			sched.markCompleted(res.index)
			
			donePieces++

			percent := float64(donePieces) / float64(len(t.PieceHashes)) * 100
			numWorkers := runtime.NumGoroutine() - 1 // subtract main thread

			log.Printf("(%0.2f%%) Downloaded piece #%d from %d peers", percent, res.index, numWorkers)
		
		case <-workerDone:
			activeWorkers--
			if activeWorkers == 0 && sched.completedCount() < len(t.PieceHashes) {
				return fmt.Errorf("all peers disconnected before completion")
			}
		}
	}
	
	// Print benchmark results
	heapMB := float64(peakHeap) / 1024.0 / 1024.0
	torrentMB := float64(t.Length) / 1024.0 / 1024.0
	
	fmt.Println("================ BENCHMARK RESULTS ================")
	fmt.Printf("Torrent Size (Z): %.2f MB\n", torrentMB)
	fmt.Printf("Peak Heap Usage (X): %.2f MB\n", heapMB)
	fmt.Printf("Max Concurrent Peers (Y): %d\n", maxActiveWorkers)
	fmt.Println("=============================================")
	
	return nil
}
