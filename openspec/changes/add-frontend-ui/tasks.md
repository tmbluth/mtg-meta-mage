## 1. Backend Updates

- [x] 1.1 Update `ConversationState` in `state.py`: rename `days` to `current_days`, add `previous_days`
- [x] 1.2 Update `ChatContext` in `routes.py`: rename `days` to `current_days`, add `previous_days`
- [x] 1.3 Update `_apply_context` in `routes.py` to handle new parameters
- [x] 1.4 Update `enforce_blocking` in `graph.py` to check `current_days` instead of `days`
- [x] 1.5 Update `summarize_state_for_ui` in `state.py` to return `current_days` instead of `days`
- [x] 1.6 Update SSE state event to emit `current_days` instead of `days`
- [x] 1.7 Run existing tests and fix any broken references to `days`

## 2. Frontend Implementation

- [x] 2.1 Create `src/app/ui/` directory structure with `app.py`, `components/`, `utils/`
- [x] 2.2 Implement SSE streaming client in `utils/sse_client.py`
- [x] 2.3 Implement main app layout with format dropdown (hidden chat until format selected)
- [x] 2.4 Implement meta research controls: `current_days` slider, `previous_days` slider
- [x] 2.5 Implement deck coaching panel: archetype dropdown, decklist textarea (prepopulated), days slider
- [x] 2.6 Implement chat interface with `st.chat_message` and `st.chat_input`
- [x] 2.7 Handle SSE event types: metadata, thinking, tool_call, content, state, done
- [x] 2.8 Implement conditional UI reveal for deck coaching controls when intent detected

## 3. Testing

- [x] 3.1 Add unit tests for updated backend schemas
- [x] 3.2 Add integration test for frontend-to-backend flow
- [x] 3.3 Update Postman collection with new parameter names

