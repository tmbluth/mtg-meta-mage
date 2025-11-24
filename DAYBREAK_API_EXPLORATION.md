# Daybreak Census API Exploration Report

**Date:** 2025-01-21  
**Service ID:** s:metamage  
**API Base URL:** https://census.daybreakgames.com/get/mtgo:v1/

## Summary

The Daybreak Census API for MTGO provides access to tournament calendar/schedule data, but **does not appear to provide tournament results, standings, decklists, or match data**. The API primarily contains:
- Scheduled/upcoming tournament information
- Calendar events for future tournaments
- Leaderboard data (requires additional parameters not yet explored)

## Available Collections

### 1. `tournament_calendar`

**Purpose:** Tournament schedule/calendar data for MTGO tournaments

**Fields Available:**
- `tournamentid` (TEXT) - Unique tournament identifier
- `tournamentrecordtypecd` (TEXT) - Record type code (e.g., "ACTL")
- `description` (TEXT) - Tournament description/name
- `tournamenttypecd` (TEXT) - Tournament type code (e.g., "PEVENT")
- `tournamentstructurecd` (TEXT) - Structure type (e.g., "SWISS")
- `playerformatcd` (TEXT) - Player format (e.g., "D1V1" = 1v1)
- `gamestructurecd` (TEXT) - Game structure/format code (e.g., "CSTANDARD", "CPAUPER", "SCHKBOKSOK")
- `matchtypecd` (TEXT) - Match type code (e.g., "BES")
- `starttime` (TEXT) - Start time in format "YYYY-MM-DD HH:MM:SS.SSS"
- `endtime` (TEXT) - End time in format "YYYY-MM-DD HH:MM:SS.SSS"
- `minimumplayers` (TEXT) - Minimum number of players
- `maximumplayers` (TEXT) - Maximum number of players
- `deckbuildinglength` (TEXT) - Deck building time in minutes
- `roundlength` (TEXT) - Round length in minutes
- `numofrounds` (TEXT) - Number of rounds
- `channelid` (TEXT) - Channel ID
- `entryfeesuitehandle` (TEXT) - Entry fee suite handle
- `keepproduct` (TEXT) - Keep product flag ("true"/"false")
- `gamesvroptionsid` (TEXT) - Game server options ID

**Query Parameters:**
- `c:limit` - Limit number of results
- `c:show` - Show specific fields only
- `c:hide` - Hide specific fields
- `tournamentid` - Filter by tournament ID
- `starttime` - Filter by start time (format: "YYYY-MM-DD HH:MM:SS" or comparison operators)
- `endtime` - Filter by end time
- `gamestructurecd` - Filter by game structure code
- `tournamenttypecd` - Filter by tournament type
- `playerformatcd` - Filter by player format
- `tournamentstructurecd` - Filter by tournament structure
- `matchtypecd` - Filter by match type
- `channelid` - Filter by channel ID
- `description` - Filter by description

**Valid Sort Fields:** All searchable fields listed above

**Sample Query:**
```bash
curl "https://census.daybreakgames.com/s:metamage/get/mtgo:v1/tournament_calendar?c:limit=5&c:show=tournamentid,description,starttime,endtime,gamestructurecd"
```

**Observations:**
- Data appears to be historical (oldest sample: 2009-02-07)
- Contains scheduled tournament information, not results
- Format codes are cryptic (e.g., "SCHKBOKSOK", "S3MED3ME2", "CSTANDARD")
- No player standings, decklists, or match results available

---

### 2. `calendars`

**Purpose:** List of available calendar types for MTGO events

**Fields Available:**
- `calendar_id` (UUID) - Unique calendar identifier
- `name` (TEXT) - Calendar name (e.g., "Public - Key Dates", "Public - Future Premier")

**Sample Calendars Found:**
- `3ada6a92-f083-499e-97bf-14f69a9f0169` - "Public - Key Dates"
- `dddf4375-799a-4dbd-b7d2-64ca2128e4e7` - "Public - Key Dates"
- `420c8972-97b2-4ec7-bb9d-2271d11df8c0` - "Public - Alternate Play"
- `5cf57fba-f279-4d66-aab2-1b593e52b1f9` - "Public - Future Premier"
- `5138d78e-4b4f-482d-87d1-ca04bdb5513d` - "Public - Qualifying Seasons"
- `d4553245-084d-48c8-a4dc-6cfb2eda4f7d` - "Public - Qualifying Seasons"

**Query Parameters:**
- `c:limit` - Limit number of results
- `calendar_id` - Filter by calendar ID

---

### 3. `calendars_event`

**Purpose:** Calendar events (scheduled tournaments) for specific calendars

**Fields Available:**
- `id` (TEXT) - Event ID (format: "YYYYMMDDTHHMMSSZ--{number}")
- `calendar_id` (UUID) - Reference to calendar
- `title` (TEXT) - Event title (e.g., "Pioneer Super Qualifier", "Modern Showcase Challenge")
- `description` (TEXT) - Event description (often empty)
- `start` (TEXT) - Start timestamp (Unix timestamp as string)
- `end` (TEXT) - End timestamp (Unix timestamp as string)
- `all_day` (TEXT) - All day flag ("true"/"false")
- `background_color` (TEXT) - CSS color variable for background
- `text_color` (TEXT) - CSS color variable for text
- `border_color` (TEXT) - CSS color variable for border
- `secondary_border_color` (TEXT) - CSS color variable for secondary border

**Query Parameters:**
- `calendar_id` (REQUIRED) - Filter by calendar ID
- `c:limit` - Limit number of results
- `c:show` - Show specific fields only

**Sample Query:**
```bash
curl "https://census.daybreakgames.com/s:metamage/get/mtgo:v1/calendars_event?calendar_id=5cf57fba-f279-4d66-aab2-1b593e52b1f9&c:limit=5"
```

**Observations:**
- Contains future tournament schedules (dates in 2025)
- Event titles indicate format (e.g., "Pioneer", "Modern", "Standard", "Pauper", "Legacy")
- No results or standings data
- Timestamps are Unix timestamps (seconds since epoch)

**Sample Event Titles Found:**
- "Pioneer Super Qualifier"
- "Pioneer Showcase Challenge"
- "Pauper Super Qualifier"
- "Pauper Championship Finals"
- "Legacy Showcase Challenge"
- "Standard Showcase Challenge"
- "Modern Showcase Challenge"
- "EOE Limited Super Qualifier"
- "TLA Limited Super Qualifier"

---

### 4. `leaderboard`

**Purpose:** Player leaderboard/ranking data

**Status:** Requires `digitalobjectcatalogid` parameter (not yet explored)

**Error Message:**
```
"INVALID_SEARCH_TERM: missing required parameter digitalobjectcatalogid"
```

**Next Steps:** Need to identify valid `digitalobjectcatalogid` values to query leaderboard data.

---

## Data Mapping to Current Schema

### `tournament_calendar` → `tournaments` table

| Daybreak Field | Current Schema Field | Notes |
|----------------|---------------------|-------|
| `tournamentid` | `tournament_id` | Direct mapping |
| `description` | `tournament_name` | Direct mapping |
| `gamestructurecd` | `format` | Needs decoding (e.g., "CSTANDARD" → "Standard") |
| `starttime` | `start_date` | Needs parsing from "YYYY-MM-DD HH:MM:SS.SSS" |
| `endtime` | - | Not in current schema |
| `numofrounds` | - | Could map to `swiss_num` if applicable |
| `minimumplayers` / `maximumplayers` | - | Not in current schema |
| - | `top_cut` | Not available in Daybreak API |
| - | `city` | Not available in Daybreak API |
| - | `state` | Not available in Daybreak API |

### Missing Data in Daybreak API

The following data required by the current schema is **NOT available** in the Daybreak API:

1. **Player Data** (`players` table):
   - Player names
   - Wins/losses/draws
   - Standings
   - Win rates

2. **Decklist Data** (`decklists` table):
   - Decklist text
   - Card lists

3. **Match Data** (`matches` table):
   - Round-by-round match results
   - Player pairings
   - Winners

4. **Tournament Results**:
   - Final standings
   - Swiss round results
   - Top cut brackets

## Format Code Decoding

The `gamestructurecd` field uses cryptic codes. Examples found:
- `CSTANDARD` - Standard format
- `CPAUPER` - Pauper format
- `SCHKBOKSOK` - Unknown (appears in older tournaments)
- `S3MED3ME2` - Unknown (appears in older tournaments)
- `S3ALA3CON` - Sealed format
- `STSPPLCFUT` - Unknown
- `SRAVGPTDIS` - Unknown
- `C100S` - 100-card Singleton
- `CSTANDVAN` - Standard with Vanguard

## Recommendations

1. **Limited Integration Potential**: The Daybreak API appears to only provide tournament schedules, not results. This means it cannot fully replace TopDeck.gg as a data source for tournament results, standings, decklists, or matches.

2. **Possible Use Cases**:
   - Supplement tournament calendar with scheduled MTGO events
   - Track upcoming tournaments for planning purposes
   - Cross-reference tournament IDs between systems

3. **Schema Considerations**:
   - If integrating, consider adding a `source` field to `tournaments` table to distinguish "topdeck" vs "daybreak" tournaments
   - May need separate tables for scheduled vs completed tournaments
   - Format code decoding will be required

4. **Leaderboard Exploration**: The `leaderboard` collection requires further investigation to determine if it contains useful player ranking data.

5. **Alternative Approach**: Consider using Daybreak API only for:
   - Upcoming tournament schedules
   - Tournament metadata (format, dates, structure)
   - Not for results/standings/decklists

## API Rate Limiting

According to Daybreak API documentation:
- Service ID "s:example" is throttled to 10 requests per minute per client IP
- Custom service IDs may have different limits
- Always use a service ID in requests

## Next Steps

1. Explore `leaderboard` collection with valid `digitalobjectcatalogid` values
2. Determine if there are other endpoints for tournament results
3. Check if tournament IDs from `tournament_calendar` can be used to fetch results from other sources
4. Consider format code decoding strategy if integration proceeds


