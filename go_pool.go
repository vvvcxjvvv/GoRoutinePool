package goroutine_pool

import (
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrQueueFull  = errors.New("timeout submitting task, task queue is full")
	ErrPoolClosed = errors.New("pool is closed")
)

// Task 定义任务结构体，包含执行函数、参数和结果通道
type Task struct {
	// 执行任务的函数
	Execute func(args ...interface{}) interface{}
	// 任务的参数
	Args []interface{}
	// 用于接收任务结果的通道
	Result chan interface{}
	// 任务是否被正确处理的标志位
	Completed bool
}

// 工作协程
type Worker struct {
	pool          *Pool     // 所属的协程池
	taskChan      chan Task // 工作通道
	isInterrupted bool      // 是否被stop打断
}

// 协程池
type Pool struct {
	maxWorkerNum   uint32         // 最大工作协程数量
	maxQueueSize   uint32         // 任务队列最大长度
	taskChan       chan Task      // 任务队列
	workers        []*Worker      // 工作协程列表
	idleWorkers    chan *Worker   // 空闲协程列表
	stopChan       chan struct{}  // 用于通知协程池停止
	currentWorkers int32          // 当前工作协程数量
	mutex          sync.Mutex     // 互斥锁，用于保护对协程池状态的操作
	wg             sync.WaitGroup // 同步等待组
	isStopped      bool           // 新增停止状态标识
	stopOnce       sync.Once      // 确保只关闭一次
}

// Run 启动工作协程，使其开始从任务通道中获取任务并执行
func (w *Worker) Run() {
	go func() {
		defer func() { // 监听到stopChan关闭信号时才会执行
			close(w.taskChan) // 由worker自己关闭通道
			w.pool.wg.Done()
			atomic.AddInt32(&w.pool.currentWorkers, -1)
			log.Printf("recycle worker, currentWorkers is %d", w.pool.currentWorkers)
		}()
		for {
			select {
			case task, _ := <-w.taskChan:
				// 处理任务
				func() {
					// 捕获可能的panic
					defer func() {
						if r := recover(); r != nil {
							log.Printf("Task panicked: %v", r)
							close(task.Result)
						}
					}()
					// 执行任务并将结果发送到结果通道
					result := task.Execute(task.Args...)
					task.Result <- result
					// 任务标志位进行设置
					task.Completed = true
					// 不再发送消息，发送端主动关闭channel
					close(task.Result)
				}()
				// 执行完任务后，将worker放回空闲队列
				w.pool.putWorker(w)
			case <-w.pool.stopChan:
				func() {
					w.pool.mutex.Lock()
					defer w.pool.mutex.Unlock()
					w.isInterrupted = true // 监听到stopChan时将标志位置为true，防止worker.taskChan被close后还有任务传入
				}()
				return
			}
		}
	}()
}

// addWorker 添加一个新的工作协程
func (p *Pool) addWorker() {
	p.wg.Add(1)
	worker := &Worker{
		pool:          p,
		taskChan:      make(chan Task),
		isInterrupted: false,
	}
	p.workers = append(p.workers, worker) // 添加到工作池的工作队列中
	p.idleWorkers <- worker               // 添加到空闲队列中
	atomic.AddInt32(&p.currentWorkers, 1)
	log.Printf("pool add worker: %d", p.currentWorkers)
	worker.Run() // 在新的协程中等待分配并处理任务
}

// NewPool 创建一个新的协程池，指定最大工作协程数和任务队列缓冲大小
func NewPool(maxWorkerNum, maxQueueSize uint32) *Pool {
	p := &Pool{
		maxWorkerNum:   maxWorkerNum,
		maxQueueSize:   maxQueueSize,
		currentWorkers: 0,
		taskChan:       make(chan Task, maxQueueSize),
		workers:        make([]*Worker, 0, maxWorkerNum),
		idleWorkers:    make(chan *Worker, maxWorkerNum),
		stopChan:       make(chan struct{}),
	}
	log.Println("goroutine pool init...")
	// 初始化工作协程，创建指定数量的工作协程并启动
	for i := 0; i < int(maxWorkerNum); i++ {
		p.addWorker()
	}
	return p
}

// putWorker 将工作协程放回空闲队列
func (p *Pool) putWorker(w *Worker) {
	p.idleWorkers <- w
}

// getWorker 从空闲队列获取一个工作协程，如果没有空闲协程则等待
func (p *Pool) getWorker() *Worker {
	return <-p.idleWorkers
}

// Submit 提交任务到任务队列，如果任务队列已满则阻塞或超时返回错误
func (p *Pool) Submit(task Task) error {
	// 检测到pool被关闭，停止提交任务
	if p.isStopped {
		return ErrPoolClosed
	}
	select {
	case p.taskChan <- task:
		return nil
	case <-time.After(1 * time.Second):
		return ErrQueueFull
	}
}

// Start 启动协程池，开始从任务队列中取出任务并分配给空闲工作协程
func (p *Pool) Start() {
	log.Println("starting goroutine pool...")
	go func() {
		defer func() {
			log.Println("start() stopped")
		}()
		for {
			select {
			case task, ok := <-p.taskChan:
				if !ok {
					// 通道无数据且已关闭，退出循环
					log.Println("taskChan of pool is stopped, quit Start()...")
					return
				}
				worker := p.getWorker() // idleWorkers通道被关闭后，可能会返回零值nil
				func() {                // 避免worker.taskChan被回收，但是仍有任务传入
					p.mutex.Lock()
					defer p.mutex.Unlock()
					if worker != nil && !worker.isInterrupted { // 未被回收，正常分派任务
						worker.taskChan <- task
					}
					// worker.isInterrupted为true时，说明worker.taskChan已经被回收，此时放弃处理任务
				}()
			default:
			}
		}
	}()
}

// Stop 停止协程池
func (p *Pool) Stop() {
	log.Println("Stopping goroutine pool...")
	p.stopOnce.Do(func() {
		p.isStopped = true // 先将标志位设为true，此时再提交任务会直接拒绝
		close(p.taskChan)  // 停止接收新任务，此时还会将剩余的task分派给各个worker
		close(p.stopChan)  // 通知所有协程停止
		p.wg.Wait()        // 等待所有工作协程完成任务
		close(p.idleWorkers)
	})
	/**
	关闭逻辑：
	1. p.isStopped标志位置位，Submit停止提交任务
	2. p.taskChan关闭，Start将剩余任务分发给各个worker后，通道关闭且无数据触发case进行return
	3. p.stopChan关闭，触发worker的Run关闭，每个worker执行wg.Done()并回收worker.taskChan
	4. p.wg.Wait()等待所有工作协程完成任务
	5. p.idleWorkers关闭
	*/
	log.Println("Goroutine pool stopped.")
}
