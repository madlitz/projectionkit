package meilisearchprojection_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/projectionkit/internal/adaptortest"
	"github.com/dogmatiq/projectionkit/internal/identity"
	. "github.com/dogmatiq/projectionkit/meilisearchprojection"
	"github.com/dogmatiq/projectionkit/meilisearchprojection/fixtures" // can't dot-import due to conflict
	"github.com/meilisearch/meilisearch-go"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type adaptor", func() {

	var (
		ctx     context.Context
		cancel  context.CancelFunc
		handler *fixtures.MessageHandler
		db      meilisearch.ServiceManager
		adaptor dogma.ProjectionMessageHandler
	)

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)

		db = meilisearch.New("http://localhost:7700")

		handler = &fixtures.MessageHandler{}
		handler.ConfigureFunc = func(c dogma.ProjectionConfigurer) {
			c.Identity("<projection>", "<key>")
		}

		adaptor = New(db, handler, "projection_occ")

	})

	AfterEach(func() {
		task, err := db.DeleteIndexWithContext(ctx, "projection_occ")
		Expect(err).ShouldNot(HaveOccurred())
		err = waitForTask(ctx, db, task.TaskUID)
		Expect(err).ShouldNot(HaveOccurred())
		db.Close()
		cancel()
	})

	adaptortest.DescribeAdaptor(&ctx, &adaptor)

	Describe("func Configure()", func() {
		It("forwards to the handler", func() {
			Expect(identity.Key(adaptor)).To(Equal("<key>"))
		})
	})

	Describe("func HandleEvent()", func() {
		It("returns an error if the application's message handler fails", func() {
			terr := errors.New("handle event test error")

			handler.HandleEventFunc = func(
				context.Context,
				meilisearch.IndexManager,
				dogma.ProjectionEventScope,
				dogma.Event,
			) error {
				return terr
			}

			_, err := adaptor.HandleEvent(
				context.Background(),
				[]byte("<resource>"),
				nil,
				[]byte("<version 01>"),
				nil,
				EventA1,
			)
			Expect(err).Should(HaveOccurred())
		})
	})

	Describe("func Compact()", func() {
		It("forwards to the handler", func() {
			handler.CompactFunc = func(
				_ context.Context,
				d meilisearch.ServiceManager,
				_ dogma.ProjectionCompactScope,
			) error {
				Expect(d).To(BeIdenticalTo(db))
				return errors.New("<error>")
			}

			err := adaptor.Compact(
				context.Background(),
				nil, // scope
			)
			Expect(err).To(MatchError("<error>"))
		})
	})
})

func waitForTask(ctx context.Context, db meilisearch.ServiceManager, taskID int64) error {
	for {
		task, err := db.GetTaskWithContext(ctx, taskID)
		if err != nil {
			return err
		}
		if task.Status == "succeeded" {
			return nil
		}
		if task.Status == "failed" {
			return fmt.Errorf("task failed: %v", task.Error)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
