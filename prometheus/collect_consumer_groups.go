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
	groups, allGroups, err := e.minionSvc.DescribeConsumerGroups(ctx)
	if err != nil {
		e.logger.Error("failed to collect consumer groups, because Kafka request failed", zap.Error(err))
		return false
	}

	// The list of groups may be incomplete due to group coordinators that might fail to respond. We do log an error
	// message in that case (in the kafka request method) and groups will not be included in this list.
	emptyGroups := 0
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
			state := 0
			if group.State == "Stable" {
				state = 1
			}
			if group.State != "Empty" {
				// don't report on empty groups
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
			} else {
				emptyGroups++
			}

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
			topicConsumers := make(map[string]int)
			topicPartitionsAssigned := make(map[string]int)
			membersWithEmptyAssignment := 0
			failedAssignmentsDecode := 0
			for _, member := range group.Members {
				kassignment := kmsg.NewGroupMemberAssignment()
				if err := kassignment.ReadFrom(member.MemberAssignment); err != nil {
					e.logger.Debug("failed to decode consumer group member assignment, internal kafka error",
						zap.Error(err),
						zap.String("group_id", group.Group),
						zap.String("client_id", member.ClientID),
						zap.String("member_id", member.MemberID),
						zap.String("client_host", member.ClientHost),
					)
					failedAssignmentsDecode++
					continue
				}
				if len(kassignment.Topics) == 0 {
					membersWithEmptyAssignment++
				}
				for _, topic := range kassignment.Topics {
					topicConsumers[topic.Topic]++
					topicPartitionsAssigned[topic.Topic] += len(topic.Partitions)
				}
			}
			if failedAssignmentsDecode > 0 {
				e.logger.Error("failed to decode consumer group member assignment, internal kafka error",
					zap.Error(err),
					zap.String("group_id", group.Group),
					zap.Int("assignment_decode_failures", failedAssignmentsDecode),
				)
			}
			// number of members with no assignment in a stable consumer group
			if membersWithEmptyAssignment > 0 && group.State == "Stable" {
				ch <- prometheus.MustNewConstMetric(
					e.consumerGroupMembersEmpty,
					prometheus.GaugeValue,
					float64(membersWithEmptyAssignment),
					group.Group,
				)
			}
			// number of members in consumer groups for each topic
			for topicName, consumers := range topicConsumers {
				ch <- prometheus.MustNewConstMetric(
					e.consumerGroupTopicMembers,
					prometheus.GaugeValue,
					float64(consumers),
					group.Group,
					topicName,
				)
			}
			// number of partitions assigned in consumer groups for each topic
			for topicName, partitions := range topicPartitionsAssigned {
				ch <- prometheus.MustNewConstMetric(
					e.consumerGroupAssignedTopicPartitions,
					prometheus.GaugeValue,
					float64(partitions),
					group.Group,
					topicName,
				)
			}
		}
	}
	ch <- prometheus.MustNewConstMetric(
		e.consumerGroupInfoAllGroups,
		prometheus.GaugeValue,
		float64(allGroups),
	)
	ch <- prometheus.MustNewConstMetric(
		e.consumerGroupInfoEmptyGroups,
		prometheus.GaugeValue,
		float64(emptyGroups),
	)
	return true
}
