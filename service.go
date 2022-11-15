package main

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// тут вы пишете код
// обращаю ваше внимание - в этом задании запрещены глобальные переменные
// если хочется, то для красоты можно разнести логику по разным файликам

type LoggerHelpful struct {
	EventID       int
	EventDelivery []chan *Event
	mu            sync.Mutex // мьютекс для логов
}

type StatisticHelpful struct {
	StatID int
	mu     sync.Mutex
	Data   map[int]Stat
}

type MyMicroservice struct {
	UnimplementedBizServer
	UnimplementedAdminServer

	ACLData map[string][]string // уровни доступа

	l *LoggerHelpful // так как наш ресивер не указатель
	s *StatisticHelpful
}

// Реализация метода Add для dummy логики
func (mm MyMicroservice) Add(_ context.Context, _ *Nothing) (*Nothing, error) {
	return &Nothing{}, nil
}

// Реализация метода Check для dummy логики
func (mm MyMicroservice) Check(_ context.Context, _ *Nothing) (*Nothing, error) {
	return &Nothing{}, nil
}

// Реализация метода Test для dummy логики
func (mm MyMicroservice) Test(_ context.Context, _ *Nothing) (*Nothing, error) {
	return &Nothing{}, nil
}

func (mm MyMicroservice) Logging(_ *Nothing, inStream Admin_LoggingServer) error {

	mm.l.mu.Lock()

	mm.l.EventDelivery = append(mm.l.EventDelivery, make(chan *Event))
	event := mm.l.EventDelivery[mm.l.EventID]
	mm.l.EventID += 1

	mm.l.mu.Unlock()

	// читаем из канала, в который запиали на этопе ACL в интерсепторе
	// отправляем клиенту в стрминге
	for {

		e := <-event

		err := inStream.Send(e)

		if err != nil {
			return err
		}
	}
}

func (mm MyMicroservice) Statistics(timeSend *StatInterval, inStream Admin_StatisticsServer) error {

	// лочим, чтобы кто-то не поменял
	mm.s.mu.Lock()
	ID := mm.s.StatID
	mm.s.mu.Unlock()

	ticker := time.NewTicker(time.Duration(timeSend.IntervalSeconds) * time.Second)
	defer ticker.Stop()

	for {

		<-ticker.C

		// аналогичный лок
		mm.s.mu.Lock()
		tmp := mm.s.Data[ID]
		mm.s.mu.Unlock()

		tmp.Timestamp = time.Now().Unix()

		err := inStream.Send(&tmp)

		if err != nil {
			return err
		}

		// так как обнуляем значение, а кто-то может залезть
		mm.s.mu.Lock()

		delete(mm.s.Data, ID)

		mm.s.Data[ID] = Stat{
			ByConsumer: make(map[string]uint64),
			ByMethod:   make(map[string]uint64),
		}

		mm.s.mu.Unlock()
	}
}

// Создание инстанса микросервиса
func NewMyMicroservice(aclData string) (MyMicroservice, error) {

	mm := MyMicroservice{
		ACLData: make(map[string][]string, 0),

		l: &LoggerHelpful{
			mu:            sync.Mutex{},
			EventDelivery: []chan *Event{},
			EventID:       0,
		},

		s: &StatisticHelpful{
			mu:   sync.Mutex{},
			Data: make(map[int]Stat, 1),
		},
	}

	err := json.Unmarshal([]byte(aclData), &mm.ACLData)

	if err != nil {
		return MyMicroservice{}, err
	}

	return mm, nil
}

func StartMyMicroservice(ctx context.Context, addr string, aclData string) error {

	// получаем инстанс
	mm, err := NewMyMicroservice(aclData)

	if err != nil {
		return err
	}

	// сушаем
	listen, err := net.Listen("tcp", addr)

	if err != nil {
		log.Fatalln("cant listet port", err)
	}

	// регистрируем сервер для бизнес логики и логов
	server := grpc.NewServer(
		grpc.UnaryInterceptor(mm.UnaryValidateACLInterceptor),
		grpc.StreamInterceptor(mm.StreamValidateACLInterceptor),
	)

	RegisterBizServer(server, mm)
	RegisterAdminServer(server, mm)

	// запускаем в горутине сервер
	go func() {
		err := server.Serve(listen)

		if err != nil {
			log.Fatal("Problem with serve")
		}
	}()

	// запускаем в горитине проверку на отмену запроса
	go func() {
		<-ctx.Done()
		server.Stop()
	}()

	return nil
}

// интерсептор для проверки ACL для обычных (Unary) методов сервиса. Интерсептор -- это штука, которая обрабатывает
// запрос перед вызовм хендлера (вызываемая юзером функция)
func (mm MyMicroservice) UnaryValidateACLInterceptor(
	ctx context.Context, // контекст
	req interface{}, // распаршенный запрос
	info *grpc.UnaryServerInfo, // вся информация о RPC
	handler grpc.UnaryHandler, // вызываемая функция
) (interface{}, error) {

	var flag bool

	p, _ := peer.FromContext(ctx)

	// доастаем метаданные -- это просто мапка
	md, _ := metadata.FromIncomingContext(ctx)

	// смотрим, кто писал
	val, ok := md["consumer"]

	if !ok {
		return nil, status.Error(codes.Unauthenticated, "bad auth")
	}

	// если все норм, то проверяем ACL для consumer
	methods, ok := mm.ACLData[val[0]]

	if !ok {
		return nil, status.Error(codes.Unauthenticated, "bad auth")
	}

	// случай, если админ сделал запрос
	if len(methods) == 1 && len(strings.Split(methods[0], "/")) == 3 && strings.Split(methods[0], "/")[2] == "*" {
		flag = true
	}

	// смотрим все доступные методы для consumer
	if !flag {
		for _, method := range methods {
			if method == info.FullMethod {
				flag = true
				break
			}
		}
	}

	if flag {

		mm.l.mu.Lock()

		for _, c := range mm.l.EventDelivery {
			c <- &Event{
				Timestamp: time.Now().Unix(),
				Consumer:  val[0],
				Method:    info.FullMethod,
				Host:      p.Addr.String(),
			}
		}

		mm.l.mu.Unlock()

		mm.s.mu.Lock()

		// fmt.Printf("[UnaryValidateACLInterceptor] FullMethod: %s\n", info.FullMethod)

		mm.s.Data[mm.s.StatID] = Stat{
			ByConsumer: make(map[string]uint64),
			ByMethod:   make(map[string]uint64),
		}

		mm.s.StatID += 1

		// fmt.Printf("[SASASASASASASS] len(mm.s.Data) = %v", len(mm.s.Data))

		for _, s := range mm.s.Data {

			// fmt.Printf("[UnaryValidateACLInterceptor] BEFORE s.ByMethod[%v]: %v\n", info.FullMethod, s.ByMethod[info.FullMethod])
			// fmt.Printf("[UnaryValidateACLInterceptor] BEFORE s.ByConsumer[%v]: %v\n", val[0], s.ByConsumer[val[0]])
			s.ByMethod[info.FullMethod] += 1
			s.ByConsumer[val[0]] += 1

			// fmt.Printf("[UnaryValidateACLInterceptor] AFTER s.ByMethod[%v]: %v\n", info.FullMethod, s.ByMethod[info.FullMethod])
			// fmt.Printf("[UnaryValidateACLInterceptor] AFTER s.ByConsumer[%v]: %v\n", val[0], s.ByConsumer[val[0]])
		}

		mm.s.mu.Unlock()

		return handler(ctx, req)

	} else {
		return nil, status.Error(codes.Unauthenticated, "bad auth")
	}
}

// интерсептор для проверки ACL для потоковых (Stream) методов сервиса.
func (mm MyMicroservice) StreamValidateACLInterceptor(
	srv interface{},
	ss grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {

	var flag bool

	p, _ := peer.FromContext(ss.Context())

	// доастаем метаданные -- это просто мапка
	md, _ := metadata.FromIncomingContext(ss.Context())

	// смотрим, кто писал
	val, ok := md["consumer"]

	if !ok {
		return status.Error(codes.Unauthenticated, "bad auth")
	}

	// если все норм, то проверяем ACL для consumer
	methods, ok := mm.ACLData[val[0]]

	if !ok {
		return status.Error(codes.Unauthenticated, "bad auth")
	}

	// случай, если админ сделал запрос
	if len(methods) == 1 && len(strings.Split(methods[0], "/")) == 3 && strings.Split(methods[0], "/")[2] == "*" {
		flag = true
	}

	// смотрим все доступные методы для consumer
	if !flag {
		for _, method := range methods {
			if method == info.FullMethod {
				flag = true
				break
			}
		}
	}

	if flag {

		mm.l.mu.Lock()

		for _, c := range mm.l.EventDelivery {
			c <- &Event{
				Timestamp: time.Now().Unix(),
				Consumer:  val[0],
				Method:    info.FullMethod,
				Host:      p.Addr.String(),
			}
		}

		mm.l.mu.Unlock()

		mm.s.mu.Lock()

		// fmt.Printf("[UnaryValidateACLInterceptor] FullMethod: %s\n", info.FullMethod)

		mm.s.Data[mm.s.StatID] = Stat{
			ByConsumer: make(map[string]uint64),
			ByMethod:   make(map[string]uint64),
		}

		mm.s.StatID += 1

		for _, s := range mm.s.Data {

			// fmt.Printf("[UnaryValidateACLInterceptor] BEFORE s.ByMethod[%v]: %v\n", info.FullMethod, s.ByMethod[info.FullMethod])
			// fmt.Printf("[UnaryValidateACLInterceptor] BEFORE s.ByConsumer[%v]: %v\n", val[0], s.ByConsumer[val[0]])
			s.ByMethod[info.FullMethod] += 1
			s.ByConsumer[val[0]] += 1

			// fmt.Printf("[UnaryValidateACLInterceptor] AFTER s.ByMethod[%v]: %v\n", info.FullMethod, s.ByMethod[info.FullMethod])
			// fmt.Printf("[UnaryValidateACLInterceptor] AFTER s.ByConsumer[%v]: %v\n", val[0], s.ByConsumer[val[0]])
		}

		mm.s.mu.Unlock()

		return handler(srv, ss)

	} else {
		return status.Error(codes.Unauthenticated, "bad auth")
	}
}
