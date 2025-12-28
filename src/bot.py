"""
Community Pulse Bot - Main Bot File
Tracks server health and provides actionable analytics
"""

import discord
from discord.ext import commands
from discord import app_commands
import os
from datetime import datetime
import asyncio

from database.db_manager import DatabaseManager
from analytics.health_analyzer import HealthAnalyzer
from analytics.channel_analyzer import ChannelAnalyzer
from analytics.contributor_analyzer import ContributorAnalyzer

# Bot configuration
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
intents.guilds = True

bot = commands.Bot(command_prefix="!", intents=intents)
db_manager = DatabaseManager()


@bot.event
async def on_ready():
    """Bot startup event"""
    print(f"✅ {bot.user} is online!")
    print(f"📊 Connected to {len(bot.guilds)} servers")
    
    # Sync slash commands
    try:
        synced = await bot.tree.sync()
        print(f"⚡ Synced {len(synced)} slash commands")
    except Exception as e:
        print(f"❌ Failed to sync commands: {e}")
    
    # Initialize database
    await db_manager.initialize()
    
    # Start background tasks
    bot.loop.create_task(aggregate_metrics())


@bot.event
async def on_message(message):
    """Track message events (metadata only)"""
    # Ignore bot messages
    if message.author.bot:
        return
    
    # Privacy-first: only store metadata
    await db_manager.log_message(
        guild_id=message.guild.id if message.guild else None,
        channel_id=message.channel.id,
        user_id=message.author.id,
        timestamp=message.created_at,
        # NO MESSAGE CONTENT STORED
    )
    
    await bot.process_commands(message)


@bot.event
async def on_member_join(member):
    """Track member joins"""
    await db_manager.log_member_join(
        guild_id=member.guild.id,
        user_id=member.id,
        timestamp=datetime.utcnow()
    )


@bot.event
async def on_member_remove(member):
    """Track member leaves"""
    await db_manager.log_member_leave(
        guild_id=member.guild.id,
        user_id=member.id,
        timestamp=datetime.utcnow()
    )


@bot.event
async def on_guild_join(guild):
    """Initialize database for new server"""
    await db_manager.initialize_guild(guild.id)
    print(f"🎉 Joined new server: {guild.name} ({guild.id})")


# ============================================================================
# SLASH COMMANDS
# ============================================================================

@bot.tree.command(name="pulse", description="View server activity pulse")
@app_commands.describe(days="Number of days to analyze (default: 7)")
async def pulse(interaction: discord.Interaction, days: int = 7):
    """Shows activity trends and key metrics"""
    await interaction.response.defer()
    
    try:
        health_analyzer = HealthAnalyzer(db_manager)
        pulse_data = await health_analyzer.get_pulse(interaction.guild_id, days)
        
        embed = discord.Embed(
            title="📊 Server Pulse",
            description=f"Activity analysis for the last {days} days",
            color=discord.Color.blue(),
            timestamp=datetime.utcnow()
        )
        
        # Activity trend
        trend_emoji = "📈" if pulse_data['trend'] > 0 else "📉" if pulse_data['trend'] < 0 else "➡️"
        embed.add_field(
            name=f"{trend_emoji} Activity Trend",
            value=f"{pulse_data['trend']:+.1f}% vs previous period",
            inline=False
        )
        
        # Active members
        active_pct = (pulse_data['active_members'] / pulse_data['total_members'] * 100) if pulse_data['total_members'] > 0 else 0
        embed.add_field(
            name="👥 Active Members",
            value=f"{pulse_data['active_members']}/{pulse_data['total_members']} ({active_pct:.1f}%)",
            inline=True
        )
        
        # Messages
        embed.add_field(
            name="💬 Messages",
            value=f"{pulse_data['total_messages']:,}",
            inline=True
        )
        
        # Peak hours
        peak_hours = pulse_data.get('peak_hours', [])
        peak_str = ", ".join([f"{h}:00" for h in peak_hours[:3]]) if peak_hours else "Not enough data"
        embed.add_field(
            name="⏰ Peak Hours (UTC)",
            value=peak_str,
            inline=False
        )
        
        # Quiet channels
        quiet_channels = pulse_data.get('quiet_channels', [])
        if quiet_channels:
            quiet_str = "\n".join([f"<#{ch_id}>" for ch_id in quiet_channels[:5]])
            embed.add_field(
                name="💤 Quietest Channels",
                value=quiet_str,
                inline=False
            )
        
        # Add confidence warning if data is insufficient
        if pulse_data.get('low_confidence'):
            embed.set_footer(text=f"{pulse_data['confidence_warning']} | Use /health for detailed score")
        else:
            embed.set_footer(text="Use /health for detailed health score")
        
        await interaction.followup.send(embed=embed)
        
    except Exception as e:
        await interaction.followup.send(f"❌ Error: {str(e)}", ephemeral=True)


@bot.tree.command(name="health", description="View server health score")
async def health(interaction: discord.Interaction):
    """Shows comprehensive health score and recommendations"""
    await interaction.response.defer()
    
    try:
        health_analyzer = HealthAnalyzer(db_manager)
        health_data = await health_analyzer.calculate_health_score(interaction.guild_id)
        
        score = health_data['score']
        
        # Color based on score
        if score >= 80:
            color = discord.Color.green()
            emoji = "🟢"
        elif score >= 60:
            color = discord.Color.gold()
            emoji = "🟡"
        elif score >= 40:
            color = discord.Color.orange()
            emoji = "🟠"
        else:
            color = discord.Color.red()
            emoji = "🔴"
        
        embed = discord.Embed(
            title=f"{emoji} Server Health Score",
            description=f"**{score}/100**\n{health_data['summary']}",
            color=color,
            timestamp=datetime.utcnow()
        )
        
        # Breakdown
        for metric, value in health_data['metrics'].items():
            embed.add_field(
                name=metric,
                value=f"{value}/100",
                inline=True
            )
        
        # Recommendations
        if health_data.get('recommendations'):
            rec_str = "\n".join([f"• {rec}" for rec in health_data['recommendations'][:3]])
            embed.add_field(
                name="💡 Recommendations",
                value=rec_str,
                inline=False
            )
        
        await interaction.followup.send(embed=embed)
        
    except Exception as e:
        await interaction.followup.send(f"❌ Error: {str(e)}", ephemeral=True)


@bot.tree.command(name="channels", description="Analyze channel activity")
async def channels(interaction: discord.Interaction):
    """Shows which channels are active, dead, or declining"""
    await interaction.response.defer()
    
    try:
        channel_analyzer = ChannelAnalyzer(db_manager)
        channel_data = await channel_analyzer.analyze_channels(interaction.guild_id)
        
        embed = discord.Embed(
            title="📺 Channel Analysis",
            description="Activity status for the last 7 days",
            color=discord.Color.blue(),
            timestamp=datetime.utcnow()
        )
        
        # Active channels
        active = channel_data.get('active', [])
        if active:
            active_str = "\n".join([f"🔥 <#{ch['id']}> - {ch['messages']} msgs" for ch in active[:5]])
            embed.add_field(name="Active Channels", value=active_str, inline=False)
        
        # Dead channels
        dead = channel_data.get('dead', [])
        if dead:
            dead_str = "\n".join([f"💀 <#{ch['id']}> - {ch['days_inactive']} days inactive" for ch in dead[:5]])
            embed.add_field(name="Dead Channels", value=dead_str, inline=False)
        
        # Declining channels
        declining = channel_data.get('declining', [])
        if declining:
            declining_str = "\n".join([f"⚠️ <#{ch['id']}> - {ch['decline_pct']:.0f}% drop" for ch in declining[:5]])
            embed.add_field(name="Declining Channels", value=declining_str, inline=False)
        
        await interaction.followup.send(embed=embed)
        
    except Exception as e:
        await interaction.followup.send(f"❌ Error: {str(e)}", ephemeral=True)


@bot.tree.command(name="contributors", description="View top contributors")
@app_commands.describe(days="Number of days to analyze (default: 30)")
async def contributors(interaction: discord.Interaction, days: int = 30):
    """Shows members contributing value (not just spam)"""
    await interaction.response.defer()
    
    try:
        contributor_analyzer = ContributorAnalyzer(db_manager)
        contributor_data = await contributor_analyzer.get_top_contributors(
            interaction.guild_id, 
            days
        )
        
        embed = discord.Embed(
            title="🏆 Top Contributors",
            description=f"Most valuable members over the last {days} days",
            color=discord.Color.gold(),
            timestamp=datetime.utcnow()
        )
        
        for i, contributor in enumerate(contributor_data[:10], 1):
            medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
            
            embed.add_field(
                name=f"{medal} <@{contributor['user_id']}>",
                value=f"Score: {contributor['score']:.1f} | {contributor['messages']} msgs | {contributor['engagement']:.1f}% engagement",
                inline=False
            )
        
        embed.set_footer(text="Score based on message quality, engagement, and consistency")
        
        await interaction.followup.send(embed=embed)
        
    except Exception as e:
        await interaction.followup.send(f"❌ Error: {str(e)}", ephemeral=True)


@bot.tree.command(name="suggest", description="Get AI-generated suggestions")
async def suggest(interaction: discord.Interaction):
    """Provides intelligent suggestions based on server data"""
    await interaction.response.defer()
    
    try:
        health_analyzer = HealthAnalyzer(db_manager)
        suggestions = await health_analyzer.generate_suggestions(interaction.guild_id)
        
        embed = discord.Embed(
            title="💡 Smart Suggestions",
            description="AI-powered recommendations for your server",
            color=discord.Color.purple(),
            timestamp=datetime.utcnow()
        )
        
        for i, suggestion in enumerate(suggestions[:5], 1):
            embed.add_field(
                name=f"{i}. {suggestion['title']}",
                value=suggestion['description'],
                inline=False
            )
        
        await interaction.followup.send(embed=embed)
        
    except Exception as e:
        await interaction.followup.send(f"❌ Error: {str(e)}", ephemeral=True)


# ============================================================================
# BACKGROUND TASKS
# ============================================================================

async def aggregate_metrics():
    """Periodically aggregate metrics for faster queries"""
    await bot.wait_until_ready()
    
    while not bot.is_closed():
        try:
            print("🔄 Aggregating metrics...")
            await db_manager.aggregate_daily_metrics()
            print("✅ Metrics aggregated")
        except Exception as e:
            print(f"❌ Error aggregating metrics: {e}")
        
        # Run every hour
        await asyncio.sleep(3600)


# ============================================================================
# MAIN
# ============================================================================

def main():
    """Start the bot"""
    token = os.getenv("DISCORD_BOT_TOKEN")
    
    if not token:
        print("❌ Error: DISCORD_BOT_TOKEN environment variable not set")
        print("📝 Create a .env file with: DISCORD_BOT_TOKEN=your_token_here")
        return
    
    try:
        bot.run(token)
    except Exception as e:
        print(f"❌ Failed to start bot: {e}")


if __name__ == "__main__":
    main()
