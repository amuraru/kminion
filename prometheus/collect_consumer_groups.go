package prometheus

import (
	"context"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/zap"
)

func (e *Exporter) collectConsumerGroups(ctx context.Context, ch chan<- prometheus.Metric) bool {
	if !e.minionSvc.Cfg.ConsumerGroups.Enabled {
		return true
	}
	groups, err := e.minionSvc.DescribeConsumerGroups(ctx)
	if err != nil {
		e.logger.Error("failed to collect consumer groups, because Kafka request failed", zap.Error(err))
		return false
	}

	// The list of groups may be incomplete due to group coordinators that might fail to respond. We do log a error
	// message in that case (in the kafka request method) and groups will not be included in this list.
	for _, grp := range groups {
		coordinator := grp.BrokerMetadata.NodeID
		for _, group := range grp.Groups.Groups {
			err := kerr.ErrorForCode(group.ErrorCode)
			if err != nil {
				e.logger.Warn("failed to describe consumer group, internal kafka error",
					zap.Error(err),
					zap.String("group_id", group.Group),
				)
				continue
			}
			if !e.minionSvc.IsGroupAllowed(group.Group) {
				continue
			}
			state := 0
			if group.State == "Stable" {
				state = 1
			}
			ch <- prometheus.MustNewConstMetric(
				e.consumerGroupInfo,
				prometheus.GaugeValue,
				float64(state),
				group.Group,
				group.Protocol,
				group.ProtocolType,
				group.State,
				strconv.FormatInt(int64(coordinator), 10),
			)
			// total number of members in consumer groups
			if len(group.Members) > 0 {
				ch <- prometheus.MustNewConstMetric(
					e.consumerGroupMembers,
					prometheus.GaugeValue,
					float64(len(group.Members)),
					group.Group,
				)
			}

			// iterate all members and build two maps:
			// - {topic -> number-of-consumers}
			// - {topic -> number-of-partitions-assigned}
			topic_consumers := make(map[string]int)
			topic_partitions_assigned := make(map[string]int)
			for _, member := range group.Members {
				var kassignment kmsg.GroupMemberAssignment
				if err := kassignment.ReadFrom(member.MemberAssignment); err != nil {
					e.logger.Warn("failed to decode consumer group member assignment, internal kafka error",
						zap.Error(err),
						zap.String("group_id", group.Group),
					)
				} else {
					for _, topic := range kassignment.Topics {
						topic_consumers[topic.Topic]++
						topic_partitions_assigned[topic.Topic] += len(topic.Partitions)
					}
				}
			}
			// number of members in consumer groups for each topic
			for topic_name, consumers := range topic_consumers {
				ch <- prometheus.MustNewConstMetric(
					e.consumerGroupTopicMembers,
					prometheus.GaugeValue,
					float64(consumers),
					group.Group,
					topic_name,
				)
			}
			// number of partitions assigned in consumer groups for each topic
			for topic_name, partitions := range topic_consumers {
				ch <- prometheus.MustNewConstMetric(
					e.consumerGroupTopicPartitions,
					prometheus.GaugeValue,
					float64(partitions),
					group.Group,
					topic_name,
				)
			}
		}
	}
	return true
}
