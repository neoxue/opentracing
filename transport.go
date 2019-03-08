package opentracingutils

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
	jaeger "github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/thrift"

	"github.com/uber/jaeger-client-go/log"
	"github.com/uber/jaeger-client-go/thrift-gen/zipkincore"
)

// implements jaeger's transport interface
// github.com/uber/jaeger-client-go/transport.go

// This code is adapted from 'collector-http.go' from
// https://github.com/openzipkin/zipkin-go-opentracing/

// use sarama.AsyncProducer
type KafkaTransport struct {
	encode    string
	topic     string
	group     string
	logger    jaeger.Logger
	client    sarama.AsyncProducer
	batchSize int
	batch     []*zipkincore.Span
}

// KafkaBasicAuthCredentials stores credentials for HTTP basic auth.
type KafkaBasicAuthCredentials struct {
	username string
	password string
}

type KafkaOption func(*KafkaTransport)

// HTTPTimeout sets maximum timeout for http request.
func KafkaClient(topic string, group string, bootstrapservers string, saslEnabled bool, user string, password string) KafkaOption {
	return func(c *KafkaTransport) {
		c.topic = topic
		c.group = group
		//config := cluster.NewConfig()
		config := sarama.NewConfig()
		config.Net.SASL.User = user
		config.Net.SASL.Password = password
		config.Net.SASL.Enable = saslEnabled
		config.Producer.Return.Successes = true
		bootstrapserverarr := strings.Split(bootstrapservers, ",")
		producer, err := sarama.NewAsyncProducer(bootstrapserverarr, config)
		if err != nil {
			logrus.Error(err)
		}
		c.client = producer
		var (
			wg sync.WaitGroup
		)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range producer.Successes() {
				fmt.Println("hihihihi yesssss")
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			for err := range producer.Errors() {
				logrus.Error(err)
			}
		}()
	}
}

// HTTPBatchSize sets the maximum batch size, after which a collect will be
// triggered. The default batch size is 100 spans.
func KafkaBatchSize(n int) KafkaOption {
	return func(c *KafkaTransport) { c.batchSize = n }
}

// NewKafkaTransport returns a new Kafka-sarama-backend transport
//     http://hostname:9411/api/v1/spans
func NewKafkaTransport(encode string, options ...KafkaOption) (*KafkaTransport, error) {
	c := &KafkaTransport{
		encode:    encode,
		logger:    log.NullLogger,
		batchSize: 100,
		batch:     []*zipkincore.Span{},
	}

	for _, option := range options {
		option(c)
	}
	return c, nil
}

// Append implements Transport.
func (c *KafkaTransport) Append(span *jaeger.Span) (int, error) {
	zSpan := jaeger.BuildZipkinThrift(span)
	c.batch = append(c.batch, zSpan)
	if len(c.batch) >= c.batchSize {
		return c.Flush()
	}
	return 0, nil
}

// Flush implements Transport.
func (c *KafkaTransport) Flush() (int, error) {
	count := len(c.batch)
	if count == 0 {
		return 0, nil
	}
	// 异步
	err := c.send(c.batch)
	c.batch = c.batch[:0]
	return count, err
}

// Close implements Transport.
func (c *KafkaTransport) Close() error {
	return nil
}

func thriftSerialize(spans []*zipkincore.Span) (*bytes.Buffer, error) {
	t := thrift.NewTMemoryBuffer()
	p := thrift.NewTBinaryProtocolTransport(t)
	if err := p.WriteListBegin(thrift.STRUCT, len(spans)); err != nil {
		return nil, err
	}
	for _, s := range spans {
		if err := s.Write(p); err != nil {
			return nil, err
		}
	}
	if err := p.WriteListEnd(); err != nil {
		return nil, err
	}
	return t.Buffer, nil
}
func jsonSerialize(spans *zipkincore.Span) ([]byte, error) {
	bts, err := json.Marshal(spans)
	if err != nil {
		return nil, err
	}
	spanmap := map[string]interface{}{}
	json.Unmarshal(bts, &spanmap)
	timestamp := spanmap["timestamp"]
	switch timestamp.(type) {
	case float64:
		spanmap["timestamp_millis"] = int64(timestamp.(float64) / 1000)
		return json.Marshal(spanmap)
	}
	return nil, errors.New("timestamp not float64")
}

// send 分为2类
func (c *KafkaTransport) send(spans []*zipkincore.Span) error {
	str := ""
	switch c.encode {
	case "thrift":
		bytesbuffer, err := thriftSerialize(spans)
		if err != nil {
			return err
		}
		str = bytesbuffer.String()
		message := &sarama.ProducerMessage{Topic: c.topic, Value: sarama.StringEncoder(str)}
		c.client.Input() <- message
		return nil
	case "json":
		for _, span := range spans {
			bts, err := jsonSerialize(span)
			if err != nil {
				return err
			}
			str = string(bts)
			message := &sarama.ProducerMessage{Topic: c.topic, Value: sarama.StringEncoder(bts)}
			c.client.Input() <- message
		}
	}
	return nil
}

/*
func (c *KafkaTransport) send(spans []*zipkincore.Span) error {
	fmt.Println(spans)
	spansbts, _ := json.Marshal(spans)
	fmt.Println(string(spansbts))
	body, err := httpSerialize(spans)
	if err != nil {
		return err
	}
	req, err := http.NewRequest("POST", c.url, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-thrift")

	if c.httpCredentials != nil {
		req.SetBasicAuth(c.httpCredentials.username, c.httpCredentials.password)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("could not read response from collector: %s", err)
	}

	if resp.StatusCode >= http.StatusBadRequest {
		return fmt.Errorf("error from collector: code=%d body=%q", resp.StatusCode, string(respBytes))
	}

	return nil
}
*/
