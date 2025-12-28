"""
Health Analyzer for Community Pulse Bot
Calculates server health scores and provides insights
"""

from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import asyncio


class HealthAnalyzer:
    """Analyzes server health and provides actionable insights"""
    
    def __init__(self, db_manager):
        self.db_manager = db_manager
    
    async def get_pulse(self, guild_id: int, days: int = 7) -> Dict[str, Any]:
        """Get server activity pulse"""
        try:
            # Get current period stats
            current_stats = await self.db_manager.get_message_stats(guild_id, days)
            
            # Get previous period for comparison
            previous_stats = await self._get_previous_period_stats(guild_id, days)
            
            # Calculate trend
            current_messages = current_stats['total_messages']
            previous_messages = previous_stats.get('total_messages', 0)
            
            if previous_messages > 0:
                trend = ((current_messages - previous_messages) / previous_messages) * 100
            else:
                trend = 0
            
            # Get peak hours
            peak_hours = []
            if current_stats['hourly_data']:
                peak_hours = [int(hour) for hour, count in current_stats['hourly_data'][:3]]
            
            # Get quiet channels
            channel_stats = await self.db_manager.get_channel_stats(guild_id, days)
            quiet_channels = [ch['channel_id'] for ch in channel_stats[-3:]]  # Last 3 (least active)
            
            return {
                'trend': trend,
                'active_members': current_stats['active_users'],
                'total_members': current_stats['active_users'],  # Simplified for now
                'total_messages': current_messages,
                'peak_hours': peak_hours,
                'quiet_channels': quiet_channels,
                'low_confidence': current_messages < 10,
                'confidence_warning': "Limited data available" if current_messages < 10 else None
            }
        except Exception as e:
            print(f"Error in get_pulse: {e}")
            return {
                'trend': 0,
                'active_members': 0,
                'total_members': 0,
                'total_messages': 0,
                'peak_hours': [],
                'quiet_channels': [],
                'low_confidence': True,
                'confidence_warning': "Error retrieving data"
            }
    
    async def calculate_health_score(self, guild_id: int) -> Dict[str, Any]:
        """Calculate comprehensive health score"""
        try:
            # Get data for analysis
            message_stats = await self.db_manager.get_message_stats(guild_id, 7)
            channel_stats = await self.db_manager.get_channel_stats(guild_id, 7)
            user_stats = await self.db_manager.get_user_stats(guild_id, 30)
            
            # Calculate individual metrics
            activity_score = self._calculate_activity_score(message_stats)
            engagement_score = self._calculate_engagement_score(user_stats)
            diversity_score = self._calculate_diversity_score(channel_stats)
            consistency_score = self._calculate_consistency_score(message_stats)
            
            # Overall score (weighted average)
            overall_score = int(
                activity_score * 0.3 +
                engagement_score * 0.3 +
                diversity_score * 0.2 +
                consistency_score * 0.2
            )
            
            # Generate summary
            if overall_score >= 80:
                summary = "Excellent! Your server is thriving with high activity and engagement."
            elif overall_score >= 60:
                summary = "Good health! Some areas could use improvement."
            elif overall_score >= 40:
                summary = "Moderate health. Consider implementing engagement strategies."
            else:
                summary = "Needs attention. Low activity detected."
            
            # Generate recommendations
            recommendations = self._generate_recommendations(
                activity_score, engagement_score, diversity_score, consistency_score
            )
            
            return {
                'score': overall_score,
                'summary': summary,
                'metrics': {
                    'Activity': activity_score,
                    'Engagement': engagement_score,
                    'Diversity': diversity_score,
                    'Consistency': consistency_score
                },
                'recommendations': recommendations
            }
        except Exception as e:
            print(f"Error calculating health score: {e}")
            return {
                'score': 0,
                'summary': "Unable to calculate health score",
                'metrics': {},
                'recommendations': ["Check bot permissions and try again"]
            }
    
    async def generate_suggestions(self, guild_id: int) -> List[Dict[str, str]]:
        """Generate AI-powered suggestions"""
        try:
            health_data = await self.calculate_health_score(guild_id)
            suggestions = []
            
            score = health_data['score']
            
            if score < 40:
                suggestions.extend([
                    {
                        'title': 'Boost Activity',
                        'description': 'Consider hosting events or creating discussion topics to increase engagement'
                    },
                    {
                        'title': 'Welcome New Members',
                        'description': 'Set up a welcoming system to help new members feel included'
                    }
                ])
            elif score < 70:
                suggestions.extend([
                    {
                        'title': 'Diversify Channels',
                        'description': 'Create topic-specific channels to encourage different types of discussions'
                    },
                    {
                        'title': 'Regular Events',
                        'description': 'Schedule weekly events or activities to maintain consistent engagement'
                    }
                ])
            else:
                suggestions.extend([
                    {
                        'title': 'Maintain Momentum',
                        'description': 'Your server is doing great! Keep up the current strategies'
                    },
                    {
                        'title': 'Community Recognition',
                        'description': 'Consider highlighting active members to encourage continued participation'
                    }
                ])
            
            return suggestions
        except Exception as e:
            print(f"Error generating suggestions: {e}")
            return [{'title': 'Error', 'description': 'Unable to generate suggestions'}]
    
    def _calculate_activity_score(self, message_stats: Dict[str, Any]) -> int:
        """Calculate activity score based on message volume"""
        total_messages = message_stats['total_messages']
        
        # Score based on messages per day (assuming 7-day period)
        messages_per_day = total_messages / 7
        
        if messages_per_day >= 100:
            return 100
        elif messages_per_day >= 50:
            return 80
        elif messages_per_day >= 20:
            return 60
        elif messages_per_day >= 5:
            return 40
        elif messages_per_day >= 1:
            return 20
        else:
            return 0
    
    def _calculate_engagement_score(self, user_stats: List[Dict[str, Any]]) -> int:
        """Calculate engagement score based on user participation"""
        if not user_stats:
            return 0
        
        active_users = len(user_stats)
        
        # Calculate average messages per user
        total_messages = sum(user['message_count'] for user in user_stats)
        avg_messages_per_user = total_messages / active_users if active_users > 0 else 0
        
        # Score based on user engagement
        if avg_messages_per_user >= 20:
            return 100
        elif avg_messages_per_user >= 10:
            return 80
        elif avg_messages_per_user >= 5:
            return 60
        elif avg_messages_per_user >= 2:
            return 40
        elif avg_messages_per_user >= 1:
            return 20
        else:
            return 0
    
    def _calculate_diversity_score(self, channel_stats: List[Dict[str, Any]]) -> int:
        """Calculate diversity score based on channel usage"""
        if not channel_stats:
            return 0
        
        active_channels = len(channel_stats)
        
        # Score based on number of active channels
        if active_channels >= 10:
            return 100
        elif active_channels >= 7:
            return 80
        elif active_channels >= 5:
            return 60
        elif active_channels >= 3:
            return 40
        elif active_channels >= 1:
            return 20
        else:
            return 0
    
    def _calculate_consistency_score(self, message_stats: Dict[str, Any]) -> int:
        """Calculate consistency score based on hourly distribution"""
        hourly_data = message_stats.get('hourly_data', [])
        
        if not hourly_data:
            return 0
        
        # Calculate distribution evenness
        total_messages = sum(count for hour, count in hourly_data)
        if total_messages == 0:
            return 0
        
        # Simple consistency metric: more even distribution = higher score
        active_hours = len(hourly_data)
        
        if active_hours >= 12:  # Active throughout the day
            return 100
        elif active_hours >= 8:
            return 80
        elif active_hours >= 6:
            return 60
        elif active_hours >= 4:
            return 40
        elif active_hours >= 2:
            return 20
        else:
            return 0
    
    def _generate_recommendations(self, activity: int, engagement: int, diversity: int, consistency: int) -> List[str]:
        """Generate specific recommendations based on scores"""
        recommendations = []
        
        if activity < 50:
            recommendations.append("Increase overall activity with events and discussions")
        
        if engagement < 50:
            recommendations.append("Encourage more user participation with interactive content")
        
        if diversity < 50:
            recommendations.append("Create more topic-specific channels")
        
        if consistency < 50:
            recommendations.append("Promote activity during different time zones")
        
        if not recommendations:
            recommendations.append("Great job! Keep maintaining your current engagement strategies")
        
        return recommendations
    
    async def _get_previous_period_stats(self, guild_id: int, days: int) -> Dict[str, Any]:
        """Get stats for the previous period for comparison"""
        try:
            # This is a simplified version - in a real implementation,
            # you'd query for the previous period's data
            return {'total_messages': 0}
        except Exception:
            return {'total_messages': 0}