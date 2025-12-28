"""
Contributor Analyzer for Community Pulse Bot
Identifies and ranks valuable community contributors
"""

from datetime import datetime, timedelta
from typing import Dict, List, Any
import asyncio


class ContributorAnalyzer:
    """Analyzes user contributions and identifies top contributors"""
    
    def __init__(self, db_manager):
        self.db_manager = db_manager
    
    async def get_top_contributors(self, guild_id: int, days: int = 30) -> List[Dict[str, Any]]:
        """Get top contributors based on quality metrics"""
        try:
            # Get user statistics
            user_stats = await self.db_manager.get_user_stats(guild_id, days)
            
            if not user_stats:
                return []
            
            # Calculate contributor scores
            contributors = []
            
            for user in user_stats:
                score = self._calculate_contributor_score(user, user_stats)
                
                # Calculate engagement rate
                engagement = self._calculate_engagement_rate(user)
                
                contributors.append({
                    'user_id': user['user_id'],
                    'score': score,
                    'messages': user['message_count'],
                    'channels_used': user['channels_used'],
                    'engagement': engagement,
                    'consistency': self._calculate_consistency(user),
                    'first_message': user['first_message'],
                    'last_message': user['last_message']
                })
            
            # Sort by score (highest first)
            contributors.sort(key=lambda x: x['score'], reverse=True)
            
            return contributors[:20]  # Top 20 contributors
            
        except Exception as e:
            print(f"Error getting top contributors: {e}")
            return []
    
    async def analyze_contributor_trends(self, guild_id: int, user_id: int, days: int = 30) -> Dict[str, Any]:
        """Analyze trends for a specific contributor"""
        try:
            user_stats = await self.db_manager.get_user_stats(guild_id, days)
            user_data = next((user for user in user_stats if user['user_id'] == user_id), None)
            
            if not user_data:
                return {
                    'trend': 'no_data',
                    'score': 0,
                    'rank': 0,
                    'recommendations': ['No activity data available']
                }
            
            # Calculate current score and rank
            all_contributors = await self.get_top_contributors(guild_id, days)
            user_rank = next((i + 1 for i, contrib in enumerate(all_contributors) if contrib['user_id'] == user_id), 0)
            
            score = self._calculate_contributor_score(user_data, [user_data])
            
            # Determine trend (simplified)
            if score >= 80:
                trend = 'excellent'
                recommendations = ['Outstanding contributor!', 'Consider community recognition']
            elif score >= 60:
                trend = 'good'
                recommendations = ['Great contributions', 'Keep up the engagement']
            elif score >= 40:
                trend = 'moderate'
                recommendations = ['Good participation', 'Try engaging in more channels']
            else:
                trend = 'low'
                recommendations = ['Encourage more participation', 'Welcome them to discussions']
            
            return {
                'trend': trend,
                'score': score,
                'rank': user_rank,
                'total_contributors': len(all_contributors),
                'messages': user_data['message_count'],
                'channels_used': user_data['channels_used'],
                'recommendations': recommendations
            }
            
        except Exception as e:
            print(f"Error analyzing contributor trends: {e}")
            return {
                'trend': 'error',
                'score': 0,
                'rank': 0,
                'recommendations': ['Error analyzing contributor']
            }
    
    async def identify_rising_stars(self, guild_id: int, days: int = 14) -> List[Dict[str, Any]]:
        """Identify new or rapidly improving contributors"""
        try:
            # Get recent contributors
            recent_contributors = await self.get_top_contributors(guild_id, days)
            
            # Filter for rising stars (high activity in short time)
            rising_stars = []
            
            for contributor in recent_contributors:
                # Simple heuristic: high messages per day ratio
                messages_per_day = contributor['messages'] / days
                
                if messages_per_day >= 2 and contributor['channels_used'] >= 2:
                    rising_stars.append({
                        'user_id': contributor['user_id'],
                        'messages': contributor['messages'],
                        'messages_per_day': round(messages_per_day, 1),
                        'channels_used': contributor['channels_used'],
                        'potential': 'high' if messages_per_day >= 5 else 'medium'
                    })
            
            # Sort by messages per day
            rising_stars.sort(key=lambda x: x['messages_per_day'], reverse=True)
            
            return rising_stars[:10]  # Top 10 rising stars
            
        except Exception as e:
            print(f"Error identifying rising stars: {e}")
            return []
    
    def _calculate_contributor_score(self, user: Dict[str, Any], all_users: List[Dict[str, Any]]) -> float:
        """Calculate a comprehensive contributor score"""
        try:
            messages = user['message_count']
            channels_used = user['channels_used']
            
            # Base score from message count (0-40 points)
            max_messages = max(u['message_count'] for u in all_users) if all_users else 1
            message_score = min(40, (messages / max_messages) * 40) if max_messages > 0 else 0
            
            # Channel diversity score (0-30 points)
            max_channels = max(u['channels_used'] for u in all_users) if all_users else 1
            channel_score = min(30, (channels_used / max_channels) * 30) if max_channels > 0 else 0
            
            # Consistency score (0-20 points)
            consistency_score = self._calculate_consistency(user) * 0.2
            
            # Engagement quality score (0-10 points)
            engagement_score = self._calculate_engagement_rate(user) * 0.1
            
            total_score = message_score + channel_score + consistency_score + engagement_score
            
            return round(total_score, 1)
            
        except Exception as e:
            print(f"Error calculating contributor score: {e}")
            return 0.0
    
    def _calculate_engagement_rate(self, user: Dict[str, Any]) -> float:
        """Calculate user engagement rate"""
        try:
            messages = user['message_count']
            channels = user['channels_used']
            
            # Simple engagement metric: messages spread across channels
            if channels == 0:
                return 0.0
            
            # Higher score for more even distribution across channels
            avg_messages_per_channel = messages / channels
            
            # Normalize to 0-100 scale
            if avg_messages_per_channel >= 10:
                return 100.0
            elif avg_messages_per_channel >= 5:
                return 80.0
            elif avg_messages_per_channel >= 2:
                return 60.0
            elif avg_messages_per_channel >= 1:
                return 40.0
            else:
                return 20.0
                
        except Exception as e:
            print(f"Error calculating engagement rate: {e}")
            return 0.0
    
    def _calculate_consistency(self, user: Dict[str, Any]) -> float:
        """Calculate user consistency score"""
        try:
            # Simple consistency metric based on time span
            first_message = user.get('first_message')
            last_message = user.get('last_message')
            
            if not first_message or not last_message:
                return 50.0  # Default score
            
            # Parse datetime strings if needed
            if isinstance(first_message, str):
                first_message = datetime.fromisoformat(first_message.replace('Z', '+00:00'))
            if isinstance(last_message, str):
                last_message = datetime.fromisoformat(last_message.replace('Z', '+00:00'))
            
            # Calculate time span
            time_span = (last_message - first_message).days
            
            # Longer consistent activity = higher score
            if time_span >= 14:
                return 100.0
            elif time_span >= 7:
                return 80.0
            elif time_span >= 3:
                return 60.0
            elif time_span >= 1:
                return 40.0
            else:
                return 20.0
                
        except Exception as e:
            print(f"Error calculating consistency: {e}")
            return 50.0  # Default score