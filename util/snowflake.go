package util

import (
	"errors"
	"sync"
	"time"
)

const (
	workerBits  uint8  = 10                      // 每台机器(节点)的ID位数 10位最大可以有2^10=1024个节点
	numberBits  uint8  = 12                      // 表示每个集群下的每个节点，1毫秒内可生成的id序号的二进制位数 即每毫秒可生成 2^12-1=4096个唯一ID
	workerMax   uint64 = -1 ^ (-1 << workerBits) // 节点ID的最大值，用于防止溢出
	numberMax   uint64 = -1 ^ (-1 << numberBits) // 同上，用来表示生成id序号的最大值
	timeShift   uint8  = workerBits + numberBits // 时间戳向左的偏移量
	workerShift uint8  = numberBits              // 节点ID向左的偏移量
	// 41位字节作为时间戳数值的话 大约68年就会用完
	// 假如你2010年1月1日开始开发系统 如果不减去2010年1月1日的时间戳 那么白白浪费40年的时间戳啊！
	// 这个一旦定义且开始生成ID后千万不要改了 不然可能会生成相同的ID
	epoch uint64 = 1724896773911 // 在写这个变量时的时间戳
)

type Worker struct {
	mu        sync.Mutex
	timestamp uint64
	workerId  uint64
	number    uint64
}

func NewWorker(workerId uint64) (*Worker, error) {
	if workerId > workerMax {
		return nil, errors.New("Worker ID excess of quantity")
	}

	return &Worker{
		timestamp: 0,
		workerId:  workerId,
		number:    0,
	}, nil
}

func (w *Worker) GetId() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 获取生成时的时间戳
	now := uint64(float64(time.Now().UnixMilli()))
	if w.timestamp == now {
		w.number++

		// 如果当前工作节点在1毫秒内生成的ID已经超过上限 需要等待1毫秒再继续生成
		if w.number > numberMax {
			for now <= w.timestamp {
				now = uint64(time.Now().UnixNano() / 1e6)
			}
		}
	} else {
		// 如果当前时间与工作节点上一次生成ID的时间不一致 则需要重置工作节点生成ID的序号
		w.number = 0
		w.timestamp = now // 将机器上一次生成ID的时间更新为当前时间
	}

	// 第一段 now - epoch 为该算法目前已经运行了xxx毫秒
	// 如果在程序跑了一段时间修改了epoch这个值 可能会导致生成相同的ID
	ID := uint64((now-epoch)<<timeShift | (w.workerId << workerShift) | (w.number))
	return ID
}

func (w *Worker) GetWorkerId() uint64 {
	return w.workerId
}
