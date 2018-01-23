package pubsub

// Pub is a channel based message pubsub
type Pub struct {
	messages chan<- interface{}
	sub      chan<- *sub
}

// Send sends a message to all subscribers
func (p *Pub) Send(msg interface{}) {
	p.messages <- msg
}

// Close closes the Pub
func (p *Pub) Close() {
	close(p.messages)
}

func subscribeAll(interface{}) bool {
	return true
}

type sub struct {
	messages chan<- interface{}
	cancel   <-chan struct{}
}

// Subscribe creates a channel receiving updates based on a filter.
// If filter is nil all messages pass through.
// Messages are not queued, if a message is not received until the update it is dropped
func (p *Pub) Subscribe(queueSize int) *Sub {
	if queueSize < 1 {
		queueSize = 1
	}
	messages := make(chan interface{}, queueSize)
	done := make(chan struct{})
	s := Sub{
		messages: messages,
		cancel:   done,
	}
	p.sub <- &sub{
		messages: messages,
		cancel:   done,
	}
	return &s
}

func runPub(pub, ch <-chan interface{}, subscribe <-chan *sub, filter func(interface{}) bool) {
	if filter == nil {
		filter = subscribeAll
	}
	var subs []*sub
	defer func() {
		for _, s := range subs {
			close(s.messages)
		}
	}()
	send := func(msg interface{}) {
		j := 0
		for _, s := range subs {
			select {
			case <-s.cancel:
				close(s.messages)
				continue
			default:
				subs[j] = s
				j++
				select {
				case s.messages <- msg:
				default:
				}
			}
		}
		subs = subs[:j]
	}
	for {
		select {
		case s, ok := <-subscribe:
			if !ok {
				return
			}
			subs = append(subs, s)
		case msg, ok := <-ch:
			if !ok {
				return
			}
			send(msg)
		case msg, ok := <-pub:
			if !ok {
				return
			}
			if !filter(msg) {
				continue
			}
			send(msg)
		}
	}

}

// New creates a Pub
func New() *Pub {
	pub := make(chan interface{})
	sub := make(chan *sub)
	go runPub(pub, nil, sub, nil)
	p := Pub{pub, sub}
	return &p
}

// Sub is a subscription to a Pub or Filter
type Sub struct {
	messages <-chan interface{}
	cancel   chan<- struct{}
}

// Messages returns a channel for messages
func (s *Sub) Messages() <-chan interface{} {
	return s.messages
}

// Cancel aborts the subscription
func (s *Sub) Cancel() {
	close(s.cancel)
}

// Message returns the next message in the subscription
func (s *Sub) Message() (msg interface{}, ok bool) {
	msg, ok = <-s.messages
	return
}

// Filter is a filtered pub
type Filter struct {
	pub *Pub
	sub *Sub
}

// Filter creates a subscribable filter for a Pub
func (p *Pub) Filter(filter func(interface{}) bool) *Filter {
	fsub := p.Subscribe(1)
	subscr := make(chan *sub)
	pub := Pub{
		sub: subscr,
	}
	go runPub(fsub.messages, nil, subscr, filter)
	f := Filter{
		pub: &pub,
		sub: fsub,
	}
	return &f
}

// Close aborts the subscription to the parent Pub
func (f *Filter) Close() {
	f.sub.Cancel()
}

// Subscribe adds a new subscriber to the filter
func (f *Filter) Subscribe(queueSize int) *Sub {
	return f.pub.Subscribe(queueSize)
}
