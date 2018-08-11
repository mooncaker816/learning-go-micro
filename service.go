package micro

import (
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/micro/cli"
	"github.com/micro/go-log"
	"github.com/micro/go-micro/client"
	"github.com/micro/go-micro/cmd"
	"github.com/micro/go-micro/metadata"
	"github.com/micro/go-micro/server"
)

// [Min] 一个 service 可以看成是一个服务的集合和总控（服务端，客户端，服务注册与发现，传输机制，消息队列等等）
// [Min] Options 就是对应的各个组件以及相关参数
type service struct {
	opts Options

	once sync.Once
}

// [Min] 新建一个service
func newService(opts ...Option) Service {
	// [Min] 首先新建默认的 Options 并且按照 Option 函数对相应的 Options 字段进行修改
	options := newOptions(opts...)

	// [Min] 在 client 实例外面包一个 metadata
	options.Client = &clientWrapper{
		options.Client,
		// [Min] nil map，可以通过字面量初始化，
		// [Min] 但 map[key] = value 会 panic
		metadata.Metadata{
			// [Min] key : "X-Micro-From-Service"
			// [Min] value : server name
			HeaderPrefix + "From-Service": options.Server.Options().Name,
		},
	}

	return &service{
		opts: options,
	}
}

// [Min] 按设定好的时间间隔，重新注册 server
func (s *service) run(exit chan bool) {
	if s.opts.RegisterInterval <= time.Duration(0) {
		return
	}

	// [Min] 按设定的 RegisterInterval 开启一个 ticker
	t := time.NewTicker(s.opts.RegisterInterval)

	for {
		select {
		// [Min] 时间到了就重新注册 server
		case <-t.C:
			err := s.opts.Server.Register()
			if err != nil {
				log.Log("service run Server.Register error: ", err)
			}
		// [Min] 如果收到退出信号，关闭 ticker 并退出
		case <-exit:
			t.Stop()
			return
		}
	}
}

// Init initialises options. Additionally it calls cmd.Init
// which parses command line flags. cmd.Init is only called
// on first Init.
/* [Min]
初始化 service 的参数
1. 按 Option 函数对 service 进行修改
2. 再按启动服务的 CMD flag 对 service 对的参数进行修改（会覆盖1中的修改），
只在第一次 Init 时调用一次
*/
func (s *service) Init(opts ...Option) {
	// process options
	for _, o := range opts {
		o(&s.opts)
	}

	s.once.Do(func() {
		// save user action
		action := s.opts.Cmd.App().Action

		// set service action
		s.opts.Cmd.App().Action = func(c *cli.Context) {
			// set register interval
			if i := time.Duration(c.GlobalInt("register_interval")); i > 0 {
				s.opts.RegisterInterval = i * time.Second
			}

			// user action
			action(c)
		}

		// Initialise the command flags, overriding new service
		_ = s.opts.Cmd.Init(
			cmd.Broker(&s.opts.Broker),
			cmd.Registry(&s.opts.Registry),
			cmd.Transport(&s.opts.Transport),
			cmd.Client(&s.opts.Client),
			cmd.Server(&s.opts.Server),
		)
	})
}

// [Min] 返回 service 的 Options
func (s *service) Options() Options {
	return s.opts
}

// [Min] 返回 service 的 client
func (s *service) Client() client.Client {
	return s.opts.Client
}

// [Min] 返回 service 的 server
func (s *service) Server() server.Server {
	return s.opts.Server
}

func (s *service) String() string {
	return "go-micro"
}

// [Min] 启动 service
func (s *service) Start() error {
	// [Min] 首先执行 BeforeStart 函数
	for _, fn := range s.opts.BeforeStart {
		if err := fn(); err != nil {
			return err
		}
	}

	// [Min] 启动 Server
	if err := s.opts.Server.Start(); err != nil {
		return err
	}

	// [Min] 注册 Server
	if err := s.opts.Server.Register(); err != nil {
		return err
	}

	// [Min] 调用 AfterStart 函数
	for _, fn := range s.opts.AfterStart {
		if err := fn(); err != nil {
			return err
		}
	}

	return nil
}

// [Min] 停止 service
func (s *service) Stop() error {
	var gerr error

	// [Min] 首先执行 BeforeStop 函数
	for _, fn := range s.opts.BeforeStop {
		if err := fn(); err != nil {
			gerr = err
		}
	}

	// [Min] 注销 server
	if err := s.opts.Server.Deregister(); err != nil {
		return err
	}

	// [Min] 停止 server
	if err := s.opts.Server.Stop(); err != nil {
		return err
	}

	// [Min] 执行 AfterStop 函数
	for _, fn := range s.opts.AfterStop {
		if err := fn(); err != nil {
			gerr = err
		}
	}

	return gerr
}

// [Min] 正式运营服务
func (s *service) Run() error {
	// [Min] Start service，
	// [Min] 包括执行 BeforeStart，start server，注册 server，执行 AfterStart
	if err := s.Start(); err != nil {
		return err
	}

	// start reg loop
	ex := make(chan bool)
	// [Min] 开启 goroutine，按设定好的时间间隔，不断重新注册 server
	// [Min] 如果 ex 通道收到信号，表示退出，则结束 goroutine 中的 ticker 并结束 goroutine
	go s.run(ex)

	/* [Min]
	1. SIGQUIT：
	在POSIX兼容的平台，SIGQUIT是其控制终端发送到进程，
	当用户请求的过程中执行核心转储的信号。 SIGQUIT通常可以ctrl+ \。
	在Linux上，人们还可以使用Ctrl-4或虚拟控制台，SysRq yek。
	2. SIGTERM：
	SIGTERM是杀或的killall命令发送到进程默认的信号。
	它会导致一过程的终止，但是SIGKILL信号不同，它可以被捕获和解释（或忽略）的过程。
	因此，SIGTERM类似于问一个进程终止可好，让清理文件和关闭。
	因为这个原因，许多Unix系统关机期间，初始化问题SIGTERM到所有非必要的断电过程中，
	等待几秒钟，然后发出SIGKILL强行终止仍然存在任何这样的过程。
	3. SIGINT：
	符合POSIX平台，信号情报是由它的控制终端，当用户希望中断该过程发送到处理的信号。
	通常ctrl-C，但在某些系统上，“删除”字符或“break”键 -
	当进程的控制终端的用户按下中断正在运行的进程的关键SIGINT被发送。
	4. SIGKILL：
	上符合POSIX平台上，SIGKILL是发送到处理的信号以使其立即终止。
	当发送到程序，SIGKILL使其立即终止。在对比SIGTERM和SIGINT，这个信号不能被捕获或忽略，
	并且在接收过程中不能执行任何清理在接收到该信号。
	*/
	ch := make(chan os.Signal, 1)
	// [Min] 捕获以下系统退出信号
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT)

	select {
	// wait on kill signal
	// [Min] 系统退出
	case <-ch:
	// wait on context cancel
	// [Min] context cancel/timeout 等
	case <-s.opts.Context.Done():
	}

	// [Min] 此时需要关闭 ex，来通知注册 server 的 goroutine 退出
	// exit reg loop
	close(ex)

	// [Min] 调用 Stop 方法，停止服务，包括执行 BeforeStop，注销 server，停止 server，执行 AfterStop
	return s.Stop()
}
