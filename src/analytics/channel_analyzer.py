"""
Channel Analyzer for Community Pulse Bot
Analyzes channel activity and identifies trends
"""

from datetime import datetime, timedelta
from typing import Dict, List, Any
import asyncio


class ChannelAnalyzer:
    """Analyzes channel activity patterns and health"""
    
    def __init__(self, db_manager):
        self.db_manager = db_manager
    
    async def analyze_channels(self, guild_id: int, days: int = 7) -> Dict[str, List[Dict[str, Any]]]:
        """Analyze all channels and categorize them"""
        try:
            # Get channel statistics
            channel_stats = await self.db_manager.get_channel_stats(guild_id, days)
            
            if not channel_stats:
                return {
                    'active': [],
                    'dead': [],
                    'declining': []
                }
            
            # Categorize channels
            active_channels = []
            dead_channels = []
            declining_channels = []
            
            # Calculate thresholds
            total_messages = sum(ch['message_count'] for ch in channel_stats)
            avg_messages = total_messages / len(channel_stats) if channel_stats else 0
            
            for channel in channel_stats:
                channel_id = channel['channel_id']
                message_count = channel['message_count']
                unique_users = channel['unique_users']
                
                # Active channels (above average activity)
                if message_count >= avg_messages and message_count > 10:
                    active_channels.append({
                        'id': channel_id,
                        'messages': message_count,
                        'users': unique_users,
                        'engagement': round(unique_users / message_count * 100, 1) if message_count > 0 else 0
                    })
                
                # Dead channels (no activity)
                elif message_count == 0:
                    dead_channels.append({
                        'id': channel_id,
                        'messages': 0,
                        'days_inactive': days,
                        'last_activity': 'No recent activity'
                    })
                
                # Declining channels (low activity)
                elif message_count < avg_messages * 0.3:
                    decline_pct = ((avg_messages - message_count) / avg_messages * 100) if avg_messages > 0 else 0
                    declining_channels.append({
                        'id': channel_id,
                        'messages': message_count,
                        'decline_pct': decline_pct,
                        'users': unique_users
                    })
            
            # Sort by activity
            active_channels.sort(key=lambda x: x['messages'], reverse=True)
            declining_channels.sort(key=lambda x: x['decline_pct'], reverse=True)
            
            return {
                'active': active_channels[:10],  # Top 10 active
                'dead': dead_channels[:10],      # Up to 10 dead
                'declining': declining_channels[:10]  # Top 10 declining
            }
            
        except Exception as e:
            print(f"Error analyzing channels: {e}")
            return {
                'active': [],
                'dead': [],
                'declining': []
            }
    
    async def get_channel_trends(self, guild_id: int, channel_id: int, days: int = 30) -> Dict[str, Any]:
        """Get detailed trends for a specific channel"""
        try:
            # This would require more detailed time-series data
            # For now, return basic stats
            channel_stats = await self.db_manager.get_channel_stats(guild_id, days)
            
            channel_data = next((ch for ch in channel_stats if ch['channel_id'] == channel_id), None)
            
            if not channel_data:
                return {
                    'trend': 'no_data',
                    'messages': 0,
                    'users': 0,
                    'peak_times': [],
                    'recommendations': ['No activity data available']
                }
            
            # Simple trend analysis
            messages = channel_data['message_count']
            users = channel_data['unique_users']
            
            if messages > 100:
                trend = 'growing'
                recommendations = ['Channel is very active', 'Consider pinning important messages']
            elif messages > 20:
                trend = 'stable'
                recommendations = ['Good activity level', 'Encourage more user participation']
            elif messages > 0:
                trend = 'declining'
                recommendations = ['Low activity detected', 'Consider posting engaging content']
            else:
                trend = 'dead'
                recommendations = ['No recent activity', 'Channel may need revitalization']
            
            return {
                'trend': trend,
                'messages': messages,
                'users': users,
                'peak_times': [],  # Would need hourly data
                'recommendations': recommendations
            }
            
        except Exception as e:
            print(f"Error getting channel trends: {e}")
            return {
                'trend': 'error',
                'messages': 0,
                'users': 0,
                'peak_times': [],
                'recommendations': ['Error analyzing channel']
            }
    
    async def suggest_channel_improvements(self, guild_id: int) -> List[Dict[str, str]]:
        """Suggest improvements for channel structure"""
        try:
            analysis = await self.analyze_channels(guild_id)
            suggestions = []
            
            dead_count = len(analysis['dead'])
            declining_count = len(analysis['declining'])
            active_count = len(analysis['active'])
            
            if dead_count > 3:
                suggestions.append({
                    'title': 'Archive Dead Channels',
                    'description': f'Consider archiving {dead_count} inactive channels to reduce clutter'
                })
            
            if declining_count > active_count:
                suggestions.append({
                    'title': 'Revitalize Declining Channels',
                    'description': 'Post engaging content in declining channels or merge similar topics'
                })
            
            if active_count < 3:
                suggestions.append({
                    'title': 'Create Topic Channels',
                    'description': 'Add more specific topic channels to encourage focused discussions'
                })
            
            if not suggestions:
                suggestions.append({
                    'title': 'Channel Health Good',
                    'description': 'Your channel structure appears to be working well'
                })
            
            return suggestions
            
        except Exception as e:
            print(f"Error generating channel suggestions: {e}")
            return [{'title': 'Error', 'description': 'Unable to analyze channels'}]