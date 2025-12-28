"""
Health Analyzer - Calculate server health scores and provide insights
"""

from datetime import datetime, timedelta
from typing import Dict, List
import math


class HealthAnalyzer:
    """Analyzes server health and generates actionable insights"""
    
    def __init__(self, db_manager):
        self.db = db_manager
    
    async def get_pulse(self, guild_id: int, days: int = 7) -> Dict:
        """Get quick pulse metrics for the server"""
        
        # Get basic metrics
        total_messages = await self.db.get_message_count(guild_id, days)
        active_members = await self.db.get_active_users(guild_id, days)
        trend = await self.db.get_activity_trend(guild_id, days)
        peak_hours = await self.db.get_peak_hours(guild_id, days)
        quiet_channels = await self.db.get_quiet_channels(guild_id, days)
        
        # Total members (we'd need to get this from Discord API)
        # For now, use active members as proxy
        total_members = active_members  # TODO: Get from guild object
        
        # Check data confidence
        confidence = self._calculate_confidence(total_messages, days)
        low_confidence = confidence < 0.5
        confidence_warning = None
        
        if low_confidence:
            if total_messages < 50:
                confidence_warning = "⚠️ Very limited data. Results will be more accurate after 24-72 hours of activity."
            else:
                confidence_warning = "⚠️ Limited data. Insights improve with more activity history."
        
        return {
            'total_messages': total_messages,
            'active_members': active_members,
            'total_members': total_members,
            'trend': trend,
            'peak_hours': peak_hours,
            'quiet_channels': quiet_channels,
            'days_analyzed': days,
            'confidence': confidence,
            'low_confidence': low_confidence,
            'confidence_warning': confidence_warning
        }
    
    def _calculate_confidence(self, message_count: int, days: int) -> float:
        """
        Calculate confidence score based on data volume
        Returns 0.0 to 1.0
        """
        # Need at least 10 messages per day for decent confidence
        target_messages = days * 10
        confidence = min(1.0, message_count / target_messages)
        return confidence
    
    async def calculate_health_score(self, guild_id: int) -> Dict:
        """Calculate comprehensive health score (0-100)"""
        
        # Get data for last 30 days
        messages_30d = await self.db.get_message_count(guild_id, 30)
        active_users_30d = await self.db.get_active_users(guild_id, 30)
        active_users_7d = await self.db.get_active_users(guild_id, 7)
        trend = await self.db.get_activity_trend(guild_id, 7)
        join_leave = await self.db.get_join_leave_stats(guild_id, 30)
        channel_activity = await self.db.get_channel_activity(guild_id, 30)
        
        # Check data confidence
        confidence = self._calculate_confidence(messages_30d, 30)
        low_confidence = confidence < 0.5
        
        # Calculate component scores
        scores = {}
        
        # 1. Activity Score (0-100)
        # Based on messages per active user per day
        if active_users_30d > 0:
            msgs_per_user_per_day = messages_30d / (active_users_30d * 30)
            scores['Activity'] = min(100, msgs_per_user_per_day * 20)  # 5 msgs/day = 100
        else:
            scores['Activity'] = 0
        
        # 2. Growth Score (0-100)
        # Based on trend and retention
        growth_score = 50  # Neutral baseline
        growth_score += min(25, max(-25, trend * 0.5))  # Trend impact
        growth_score += min(25, join_leave['retention_rate'] * 0.25)  # Retention impact
        scores['Growth'] = max(0, min(100, growth_score))
        
        # 3. Engagement Score (0-100)
        # Based on what % of recent members are still active
        if active_users_30d > 0:
            engagement_ratio = active_users_7d / active_users_30d
            scores['Engagement'] = engagement_ratio * 100
        else:
            scores['Engagement'] = 0
        
        # 4. Channel Health Score (0-100)
        # Based on distribution of activity across channels
        if len(channel_activity) > 0:
            total_msgs = sum(ch['message_count'] for ch in channel_activity)
            # Calculate entropy (higher = more distributed = healthier)
            entropy = 0
            for ch in channel_activity:
                if ch['message_count'] > 0:
                    p = ch['message_count'] / total_msgs
                    entropy -= p * math.log2(p)
            
            # Normalize entropy (max entropy for N channels is log2(N))
            max_entropy = math.log2(len(channel_activity)) if len(channel_activity) > 1 else 1
            channel_score = (entropy / max_entropy) * 100 if max_entropy > 0 else 50
            scores['Channel Health'] = channel_score
        else:
            scores['Channel Health'] = 0
        
        # Calculate overall score (weighted average)
        weights = {
            'Activity': 0.35,
            'Growth': 0.25,
            'Engagement': 0.25,
            'Channel Health': 0.15
        }
        
        overall_score = sum(scores[metric] * weights[metric] for metric in scores)
        
        # Generate summary with actionable guidance
        if overall_score >= 80:
            summary = "🟢 Excellent! Your server is thriving with strong engagement and healthy growth."
            priority = "Maintain momentum by continuing current strategies."
        elif overall_score >= 60:
            summary = "🟡 Good health. Your server is stable, but some areas could use improvement."
            priority = "Focus on boosting your lowest-scoring metric first."
        elif overall_score >= 40:
            summary = "🟠 Moderate health. Declining engagement detected—act now to prevent further drops."
            priority = "**Priority:** Address activity and engagement immediately."
        else:
            summary = "🔴 Critical. Your server needs immediate attention to prevent member loss."
            priority = "**Priority:** Implement all recommendations this week."
        
        # Find lowest scoring metric for targeted advice
        lowest_metric = min(scores.items(), key=lambda x: x[1])
        
        # Generate prioritized recommendations
        recommendations = []
        
        # Always add priority guidance first
        if overall_score < 80:
            recommendations.append(f"📌 {priority}")
        
        # Specific recommendations based on lowest metric
        if scores['Activity'] < 50:
            recommendations.append("🔥 **Critical:** Low activity detected. Host weekly events or start daily discussion topics.")
        
        if scores['Growth'] < 50:
            if trend < -10:
                recommendations.append("📉 **Urgent:** Activity declining fast. Review what changed in the last week (channels removed, rules changed, etc.).")
            if join_leave['retention_rate'] < 50:
                recommendations.append("👋 **High Priority:** Members leaving quickly. Create clear onboarding channel and welcome new members within 24h.")
        
        if scores['Engagement'] < 50:
            recommendations.append("💤 Most members are lurking. Try: polls, questions, contests, or recognition programs.")
        
        if scores['Channel Health'] < 50:
            if len(channel_activity) > 10:
                recommendations.append("📺 Too many channels fragment conversation. Archive channels with <10 messages/week.")
            else:
                recommendations.append("📺 Activity too concentrated. Create topic-specific channels for different interests.")
        
        # Add context about what the score means
        score_context = ""
        if overall_score < 60:
            score_context = "\n\n📊 **What This Score Means:** Scores below 60 usually indicate declining engagement. Early intervention prevents larger problems later."
        
        # Add low confidence warning
        if low_confidence:
            score_context += f"\n\n⚠️ **Data Quality:** Limited data detected. Health score accuracy improves after 24-72 hours of activity. Current confidence: {confidence*100:.0f}%"
        
        return {
            'score': round(overall_score),
            'metrics': {k: round(v) for k, v in scores.items()},
            'summary': summary + score_context,
            'recommendations': recommendations,
            'lowest_metric': lowest_metric[0],
            'confidence': confidence,
            'low_confidence': low_confidence
        }
    
    async def generate_suggestions(self, guild_id: int) -> List[Dict]:
        """Generate AI-powered suggestions based on data patterns"""
        
        suggestions = []
        
        # Get relevant data
        trend = await self.db.get_activity_trend(guild_id, 7)
        channel_activity = await self.db.get_channel_activity(guild_id, 30)
        join_leave = await self.db.get_join_leave_stats(guild_id, 30)
        peak_hours = await self.db.get_peak_hours(guild_id, 30)
        
        # Pattern 1: Declining activity
        if trend < -15:
            suggestions.append({
                'title': '📉 Activity Decline Detected',
                'description': f'Your server activity has dropped {abs(trend):.0f}% over the last week. '
                              'Consider reviewing recent changes like channel restructuring or rule updates.'
            })
        
        # Pattern 2: Dead channels
        dead_channels = [ch for ch in channel_activity if ch['message_count'] < 5]
        if len(dead_channels) > 3:
            suggestions.append({
                'title': '🗑️ Consolidate Channels',
                'description': f'You have {len(dead_channels)} nearly inactive channels. '
                              'Consider merging or archiving them to reduce fragmentation.'
            })
        
        # Pattern 3: Poor retention
        if join_leave['retention_rate'] < 60 and join_leave['joins'] > 10:
            suggestions.append({
                'title': '👋 Improve Onboarding',
                'description': f'Only {join_leave["retention_rate"]:.0f}% of new members are staying. '
                              'Create a welcome channel and ensure new members feel engaged.'
            })
        
        # Pattern 4: Timezone mismatch
        if peak_hours:
            suggestions.append({
                'title': '⏰ Optimize Event Timing',
                'description': f'Your server is most active around {peak_hours[0]:02d}:00 UTC. '
                              'Schedule important events during these peak hours for maximum engagement.'
            })
        
        # Pattern 5: High growth
        if trend > 25:
            suggestions.append({
                'title': '🚀 Capitalize on Growth',
                'description': f'Your server is growing fast ({trend:+.0f}%)! '
                              'Now is a great time to establish community guidelines and add moderators.'
            })
        
        # If no specific patterns, give general advice
        if len(suggestions) == 0:
            suggestions.append({
                'title': '✅ Server Looks Healthy',
                'description': 'No major issues detected. Keep engaging with your community regularly!'
            })
        
        return suggestions
