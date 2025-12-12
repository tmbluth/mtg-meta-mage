# Change: Add Frontend UI

## Why
Users need a visual interface to interact with the MTG Meta Mage Agent API. Currently the only way to access the system is via direct API calls. A Streamlit UI will provide format/archetype dropdowns, days sliders, decklist input, and a conversational chat interface.

## What Changes
- Add Streamlit frontend at `src/app/ui/`
- **BREAKING**: Rename `days` parameter to `current_days` in ChatContext and ConversationState
- Add `previous_days` parameter for meta research period comparison
- Update blocking logic to require `current_days` instead of `days`
- UI reveals deck coaching controls only when user expresses that intent

## Impact
- Affected specs: `agent-api` (MODIFIED requirements for days parameter naming)
- Affected code:
  - `src/app/agent_api/routes.py` - ChatContext schema
  - `src/app/agent_api/state.py` - ConversationState schema
  - `src/app/agent_api/graph.py` - Blocking enforcement
  - `src/app/ui/` - New Streamlit application

