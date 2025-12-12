## 1. Backend Updates

- [ ] 1.1 Write unit tests for updated backend schemas (`ConversationState`, `ChatContext` with `current_days`/`previous_days`)
- [ ] 1.2 Update `ConversationState` in `state.py`: rename `days` to `current_days`, add `previous_days`
- [ ] 1.3 Update `ChatContext` in `routes.py`: rename `days` to `current_days`, add `previous_days`
- [ ] 1.4 Update `_apply_context` in `routes.py` to handle new parameters
- [ ] 1.5 Write tests for `enforce_blocking` checking `current_days` instead of `days`
- [ ] 1.6 Update `enforce_blocking` in `graph.py` to check `current_days` instead of `days`
- [ ] 1.7 Update `summarize_state_for_ui` in `state.py` to return `current_days` instead of `days`
- [ ] 1.8 Update SSE state event to emit `current_days` instead of `days`
- [ ] 1.9 Run all backend tests (unit+integration) to ensure no broken references to `days`
- [ ] 1.10 Update Postman collection with new parameter names

## 2. Frontend Implementation

- [ ] 2.1 Write integration test for frontend-to-backend flow (GET /welcome, POST /chat with SSE)
- [ ] 2.2 Create `src/app/ui/` directory structure with `app.py`, `components/`, `utils/`
- [ ] 2.3 Implement SSE streaming client in `utils/sse_client.py`
- [ ] 2.4 Implement main app layout with format dropdown (hidden chat until format selected)
- [ ] 2.5 Implement meta research controls: `current_days` slider, `previous_days` slider
- [ ] 2.6 Implement deck coaching panel: archetype dropdown, decklist textarea (prepopulated), days slider
- [ ] 2.7 Implement chat interface with `st.chat_message` and `st.chat_input`
- [ ] 2.8 Handle SSE event types: metadata, thinking, tool_call, content, state, done
- [ ] 2.9 Implement conditional UI reveal for deck coaching controls when intent detected
- [ ] 2.10 Implement deck coaching blocking logic: disable/block deck coaching chat requests when archetype, decklist, or current_days are missing
- [ ] 2.11 Run integration tests to verify complete frontend-to-backend flow

