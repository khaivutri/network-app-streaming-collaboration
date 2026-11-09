package events

import "testing"

func TestBusPublishesAndUnsubscribesWithoutBlocking(t *testing.T) {
	bus := NewBus()
	id, channel := bus.Subscribe(1)
	bus.Publish("test.event", map[string]string{"value": "one"})
	select {
	case event := <-channel:
		if event.Name != "test.event" {
			t.Fatalf("event name=%q", event.Name)
		}
	default:
		t.Fatal("event was not delivered")
	}
	bus.Unsubscribe(id)
	if _, open := <-channel; open {
		t.Fatal("unsubscribed channel remained open")
	}
}

func TestBusDropsSlowSubscriberInsteadOfBlockingProducer(t *testing.T) {
	bus := NewBus()
	_, _ = bus.Subscribe(1)
	bus.Publish("first", nil)
	bus.Publish("second", nil)
}
