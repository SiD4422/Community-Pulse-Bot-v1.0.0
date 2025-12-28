"""
Database Manager for Community Pulse Bot
Handles all database operations with privacy-first approach
"""

import aiosqlite
import asyncio
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
import os


class DatabaseManager:
    """Manages database operations for the Community Pulse Bot"""
    
    def __init__(self, db_path: str = "community_pulse.db"):
        self.db_path = db_path
        self._connection = None
    
    async def initialize(self):
        """Initialize database and create tables"""
        async with aiosqlite.connect(self.db_path) as db:
            # Messages table (metadata only, no content)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    guild_id INTEGER,
                    channel_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    timestamp DATETIME NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Member events table
            await db.execute("""
                CREATE TABLE IF NOT EXISTS member_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    guild_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    event_type TEXT NOT NULL, -- 'join' or 'leave'
                    timestamp DATETIME NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Daily metrics aggregation table
            await db.execute("""
                CREATE TABLE IF NOT EXISTS daily_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    guild_id INTEGER NOT NULL,
                    date DATE NOT NULL,
                    total_messages INTEGER DEFAULT 0,
                    active_users INTEGER DEFAULT 0,
                    new_members INTEGER DEFAULT 0,
                    left_members INTEGER DEFAULT 0,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(guild_id, date)
                )
            """)
            
            # Create indexes for better performance
            await db.execute("CREATE INDEX IF NOT EXISTS idx_messages_guild_timestamp ON messages(guild_id, timestamp)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_messages_channel_timestamp ON messages(channel_id, timestamp)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_member_events_guild_timestamp ON member_events(guild_id, timestamp)")
            
            await db.commit()
            print("✅ Database initialized successfully")
    
    async def log_message(self, guild_id: Optional[int], channel_id: int, user_id: int, timestamp: datetime):
        """Log message metadata (no content stored)"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT INTO messages (guild_id, channel_id, user_id, timestamp) VALUES (?, ?, ?, ?)",
                (guild_id, channel_id, user_id, timestamp)
            )
            await db.commit()
    
    async def log_member_join(self, guild_id: int, user_id: int, timestamp: datetime):
        """Log member join event"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT INTO member_events (guild_id, user_id, event_type, timestamp) VALUES (?, ?, 'join', ?)",
                (guild_id, user_id, timestamp)
            )
            await db.commit()
    
    async def log_member_leave(self, guild_id: int, user_id: int, timestamp: datetime):
        """Log member leave event"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT INTO member_events (guild_id, user_id, event_type, timestamp) VALUES (?, ?, 'leave', ?)",
                (guild_id, user_id, timestamp)
            )
            await db.commit()
    
    async def initialize_guild(self, guild_id: int):
        """Initialize database for a new guild"""
        # Database is already initialized, just log the event
        print(f"📊 Database ready for guild {guild_id}")
    
    async def get_message_stats(self, guild_id: int, days: int = 7) -> Dict[str, Any]:
        """Get message statistics for a guild"""
        start_date = datetime.utcnow() - timedelta(days=days)
        
        async with aiosqlite.connect(self.db_path) as db:
            # Total messages
            cursor = await db.execute(
                "SELECT COUNT(*) FROM messages WHERE guild_id = ? AND timestamp >= ?",
                (guild_id, start_date)
            )
            total_messages = (await cursor.fetchone())[0]
            
            # Active users
            cursor = await db.execute(
                "SELECT COUNT(DISTINCT user_id) FROM messages WHERE guild_id = ? AND timestamp >= ?",
                (guild_id, start_date)
            )
            active_users = (await cursor.fetchone())[0]
            
            # Messages by hour
            cursor = await db.execute("""
                SELECT strftime('%H', timestamp) as hour, COUNT(*) as count
                FROM messages 
                WHERE guild_id = ? AND timestamp >= ?
                GROUP BY hour
                ORDER BY count DESC
            """, (guild_id, start_date))
            hourly_data = await cursor.fetchall()
            
            return {
                'total_messages': total_messages,
                'active_users': active_users,
                'hourly_data': hourly_data
            }
    
    async def get_channel_stats(self, guild_id: int, days: int = 7) -> List[Dict[str, Any]]:
        """Get channel activity statistics"""
        start_date = datetime.utcnow() - timedelta(days=days)
        
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("""
                SELECT channel_id, COUNT(*) as message_count, COUNT(DISTINCT user_id) as unique_users
                FROM messages 
                WHERE guild_id = ? AND timestamp >= ?
                GROUP BY channel_id
                ORDER BY message_count DESC
            """, (guild_id, start_date))
            
            results = await cursor.fetchall()
            return [
                {
                    'channel_id': row[0],
                    'message_count': row[1],
                    'unique_users': row[2]
                }
                for row in results
            ]
    
    async def get_user_stats(self, guild_id: int, days: int = 30) -> List[Dict[str, Any]]:
        """Get user activity statistics"""
        start_date = datetime.utcnow() - timedelta(days=days)
        
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("""
                SELECT user_id, COUNT(*) as message_count, 
                       COUNT(DISTINCT channel_id) as channels_used,
                       MIN(timestamp) as first_message,
                       MAX(timestamp) as last_message
                FROM messages 
                WHERE guild_id = ? AND timestamp >= ?
                GROUP BY user_id
                ORDER BY message_count DESC
            """, (guild_id, start_date))
            
            results = await cursor.fetchall()
            return [
                {
                    'user_id': row[0],
                    'message_count': row[1],
                    'channels_used': row[2],
                    'first_message': row[3],
                    'last_message': row[4]
                }
                for row in results
            ]
    
    async def aggregate_daily_metrics(self):
        """Aggregate daily metrics for all guilds"""
        yesterday = (datetime.utcnow() - timedelta(days=1)).date()
        
        async with aiosqlite.connect(self.db_path) as db:
            # Get all guilds with activity
            cursor = await db.execute("""
                SELECT DISTINCT guild_id FROM messages 
                WHERE DATE(timestamp) = ?
            """, (yesterday,))
            guilds = [row[0] for row in await cursor.fetchall()]
            
            for guild_id in guilds:
                # Count messages
                cursor = await db.execute("""
                    SELECT COUNT(*) FROM messages 
                    WHERE guild_id = ? AND DATE(timestamp) = ?
                """, (guild_id, yesterday))
                total_messages = (await cursor.fetchone())[0]
                
                # Count active users
                cursor = await db.execute("""
                    SELECT COUNT(DISTINCT user_id) FROM messages 
                    WHERE guild_id = ? AND DATE(timestamp) = ?
                """, (guild_id, yesterday))
                active_users = (await cursor.fetchone())[0]
                
                # Count new members
                cursor = await db.execute("""
                    SELECT COUNT(*) FROM member_events 
                    WHERE guild_id = ? AND event_type = 'join' AND DATE(timestamp) = ?
                """, (guild_id, yesterday))
                new_members = (await cursor.fetchone())[0]
                
                # Count left members
                cursor = await db.execute("""
                    SELECT COUNT(*) FROM member_events 
                    WHERE guild_id = ? AND event_type = 'leave' AND DATE(timestamp) = ?
                """, (guild_id, yesterday))
                left_members = (await cursor.fetchone())[0]
                
                # Insert or update daily metrics
                await db.execute("""
                    INSERT OR REPLACE INTO daily_metrics 
                    (guild_id, date, total_messages, active_users, new_members, left_members)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (guild_id, yesterday, total_messages, active_users, new_members, left_members))
            
            await db.commit()