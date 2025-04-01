package goroutine_pool

import (
	"errors"
	"log"
	"strconv"
	"sync"
	"testing"
	"time"
)

type Person struct {
	id   int
	name string
}

func TestPool1(t *testing.T) {
	wg := sync.WaitGroup{}
	// 创建一个最大工作协程数为2，任务队列缓冲大小为1的协程池
	pool := NewPool(2, 1)
	pool.Start()

	// 定义任务函数
	helloTask := func(args ...interface{}) interface{} {
		id, ok1 := args[0].(int)
		name, ok2 := args[1].(string)
		msg, ok3 := args[2].(string)
		if ok1 && ok2 && ok3 {
			newMsg := "Hello " + name + ", your id is " + strconv.Itoa(id) + ", here is your msg: " + msg + "!"
			time.Sleep(15 * time.Second)
			// 返回多个值，封装到切片中
			return []interface{}{newMsg, nil}
		}
		return []interface{}{nil, errors.New("Wrong params...")}
	}

	// 创建任务用例
	persons := []*Person{
		{id: 1, name: "chandler"},
		{id: 2, name: "monica"},
		{id: 3, name: "joey"},
		{id: 4, name: "phoebe"},
		{id: 5, name: "ross"},
		{id: 6, name: "rachel"},
	}
	tasks := make([]Task, 0)
	for _, person := range persons {
		task := Task{
			Execute:   helloTask,
			Args:      []interface{}{person.id, person.name, "你好 " + person.name},
			Result:    make(chan interface{}, 1),
			Completed: false,
		}
		tasks = append(tasks, task)
	}
	wrongTask := Task{
		Execute:   helloTask,
		Args:      []interface{}{"1", "chandler", "id is not an int type"},
		Result:    make(chan interface{}, 1),
		Completed: false,
	}
	tasks = append(tasks, wrongTask)

	//for _, task := range tasks {
	//	err := pool.Submit(task)
	//	if err != nil {
	//		t.Error(err)
	//	}
	//}

	for _, task := range tasks {
		wg.Add(1)
		task := task
		go func() {
			defer wg.Done()
			err := pool.Submit(task)
			if err != nil {
				log.Println(err)
			} else {
				log.Println("submit ok")
				// 获取任务结果
				result, ok := <-task.Result
				if !ok {
					log.Println("task result channel closed")
				} else {
					log.Println("get task result ok")
					if retChan, ok := result.([]interface{}); ok {
						msg := retChan[0]
						err := retChan[1]
						if err == nil {
							log.Println(msg)
						} else {
							log.Println(err)
						}
					}
				}
			}
		}()
	}

	// 等待所有任务完成，回收资源
	wg.Wait()
	pool.Stop()
	//select {}
}

/**
=== RUN   TestPool1
2025/03/31 22:01:36 goroutine pool init...
2025/03/31 22:01:36 pool add worker: 1
2025/03/31 22:01:36 pool add worker: 2
2025/03/31 22:01:36 starting goroutine pool...
submit ok
submit ok
submit ok
submit ok
submit ok
get task result ok
Wrong params...
timeout submitting task, task queue is full
timeout submitting task, task queue is full
get task result ok
Hello monica, your id is 2, here is your msg: 你好 monica!
get task result ok
Hello joey, your id is 3, here is your msg: 你好 joey!
get task result ok
Hello ross, your id is 5, here is your msg: 你好 ross!
get task result ok
Hello phoebe, your id is 4, here is your msg: 你好 phoebe!
2025/03/31 22:02:06 Stopping goroutine pool...
2025/03/31 22:02:06 recycle worker, currentWorkers is 1
2025/03/31 22:02:06 recycle worker, currentWorkers is 0
2025/03/31 22:02:06 Goroutine pool stopped.
2025/03/31 22:02:06 taskChan of pool is stopped, quit Start()...
--- PASS: TestPool1 (30.01s)
PASS
*/

func TestPool2(t *testing.T) {
	pool := NewPool(100, 200)
	pool.Start()

	printTask := func(args ...interface{}) interface{} {
		id, ok := args[0].(int)
		if ok {
			log.Println("id: ", id)
		}
		time.Sleep(1 * time.Second)
		return nil
	}

	var wg sync.WaitGroup
	for i := 0; i < 3000; i++ {
		wg.Add(1)
		task := Task{
			Execute:   printTask,
			Args:      []interface{}{i},
			Result:    make(chan interface{}, 1),
			Completed: false,
		}
		err := pool.Submit(task)
		if err != nil {
			wg.Done()
		} else {
			// 针对每个任务，启动一个新的协程来接收 Result 通道的结果，避免通道阻塞
			go func(resultChan chan interface{}) {
				defer wg.Done()
				// 接收结果
				<-resultChan
			}(task.Result)
		}

	}

	// 等待所有结果接收完成
	log.Println("before wg.wait()")
	wg.Wait()
	log.Println("after wg.wait()")
	// 等待所有任务完成，回收资源
	pool.Stop()
	//select {}
}
