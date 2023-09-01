package vote

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/OpenSlides/openslides-autoupdate-service/pkg/datastore/dsfetch"
	"github.com/OpenSlides/openslides-autoupdate-service/pkg/datastore/dsmock"
)

func TestPreload(t *testing.T) {
	// Tests, that the preload function needs a specific number of requests to
	// postgres.
	ctx := context.Background()

	for _, tt := range []struct {
		name        string
		data        string
		expectCount int
	}{
		{
			"one user",
			`---
			meeting/5/id: 5
			poll/1:
				meeting_id: 5
				entitled_group_ids: [30]
				pollmethod: Y
				global_yes: true
				backend: fast
				type: pseudoanonymous

			group/30/meeting_user_ids: [500]

			user/50:
				is_present_in_meeting_ids: [5]

			meeting_user/500:
				group_ids: [31]
				user_id: 50
				meeting_id: 5
			`,
			3,
		},

		{
			"Many groups",
			`---
			meeting/5/id: 5
			poll/1:
				meeting_id: 5
				entitled_group_ids: [30,31]
				pollmethod: Y
				global_yes: true
				backend: fast
				type: pseudoanonymous

			group/30/meeting_user_ids: [500]
			group/31/meeting_user_ids: [500]

			user:
				50:
					is_present_in_meeting_ids: [5]

			meeting_user/500:
				user_id: 50
				group_ids: [30]
				meeting_id: 5
			`,
			3,
		},

		{
			"Many users",
			`---
			meeting/5/id: 5
			poll/1:
				meeting_id: 5
				entitled_group_ids: [30]
				pollmethod: Y
				global_yes: true
				backend: fast
				type: pseudoanonymous

			group/30/meeting_user_ids: [500,510]

			user:
				50:
					is_present_in_meeting_ids: [5]

				51:
					is_present_in_meeting_ids: [5]

			meeting_user:
				500:
					user_id: 50
					meeting_id: 5
				510:
					user_id: 51
					meeting_id: 5
			`,
			3,
		},

		{
			"Many users in different groups",
			`---
			meeting/5/id: 5
			poll/1:
				meeting_id: 5
				entitled_group_ids: [30, 31]
				pollmethod: Y
				global_yes: true
				backend: fast
				type: pseudoanonymous

			group/30/meeting_user_ids: [500]
			group/31/meeting_user_ids: [510]

			user:
				50:
					is_present_in_meeting_ids: [5]

				51:
					is_present_in_meeting_ids: [5]

			meeting_user:
				500:
					user_id: 50
					meeting_id: 5
				510:
					user_id: 51
					meeting_id: 5
			`,
			3,
		},

		{
			"Many users in different groups with delegation",
			`---
			meeting/5/id: 5
			poll/1:
				meeting_id: 5
				entitled_group_ids: [30, 31]
				pollmethod: Y
				global_yes: true
				backend: fast
				type: pseudoanonymous

			group/30/meeting_user_ids: [500]
			group/31/meeting_user_ids: [510]

			user:
				50:
					is_present_in_meeting_ids: [5]

				51:
					is_present_in_meeting_ids: [5]

				52:
					is_present_in_meeting_ids: [5]

				53:
					is_present_in_meeting_ids: [5]

			meeting_user:
				500:
					user_id: 50
					vote_delegated_to_id: 520
					meeting_id: 5
				510:
					user_id: 51
					vote_delegated_to_id: 530
					meeting_id: 5
				520:
					user_id: 52
					meeting_id: 5
				530:
					user_id: 53
					meeting_id: 5
			`,
			4,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dsCount := dsmock.NewCounter(dsmock.Stub(dsmock.YAMLData(tt.data)))
			ds := dsmock.NewCache(dsCount)

			poll, err := loadPoll(ctx, dsfetch.New(ds), 1)
			if err != nil {
				t.Fatalf("loadPoll returned: %v", err)
			}

			dsCount.(*dsmock.Counter).Reset()

			if err := poll.preload(ctx, dsfetch.New(ds)); err != nil {
				t.Errorf("preload returned: %v", err)
			}

			if got := dsCount.(*dsmock.Counter).Count(); got != tt.expectCount {
				buf := new(bytes.Buffer)
				for _, req := range dsCount.(*dsmock.Counter).Requests() {
					fmt.Fprintln(buf, req)
				}
				t.Errorf("preload send %d requests, expected %d:\n%s", got, tt.expectCount, buf)
			}
		})
	}
}
