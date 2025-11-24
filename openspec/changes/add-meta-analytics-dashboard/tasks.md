## 1. Setup and Dependencies
- [ ] 1.1 Add fastapi, uvicorn, and polars dependencies to pyproject.toml
- [ ] 1.2 Create FastAPI application structure (main.py)
- [ ] 1.3 Create Pydantic models for request/response schemas

## 2. Meta Service Implementation
- [ ] 2.1 Write unit tests for archetype ranking calculations
- [ ] 2.2 Implement meta_service.py with archetype ranking logic
- [ ] 2.3 Write unit tests for matchup matrix calculations
- [ ] 2.4 Implement meta_service.py with matchup matrix logic
- [ ] 2.5 Write unit tests for time window filtering
- [ ] 2.6 Implement time window parameter handling

## 3. API Routes Implementation
- [ ] 3.1 Write unit tests for archetype ranking endpoint
- [ ] 3.2 Implement GET /api/v1/meta/archetypes route handler
- [ ] 3.3 Write unit tests for matchup matrix endpoint
- [ ] 3.4 Implement GET /api/v1/meta/matchups route handler
- [ ] 3.5 Add error handling and validation

## 4. Integration Testing
- [ ] 4.1 Create integration test fixtures for meta analytics
- [ ] 4.2 Write integration test for archetype ranking with real database
- [ ] 4.3 Write integration test for matchup matrix with real database
- [ ] 4.4 Test time window filtering end-to-end
- [ ] 4.5 Test filtering by color_identity and strategy

## 5. Documentation
- [ ] 5.1 Update README with API endpoint documentation
- [ ] 5.2 Document query parameters and response schemas
- [ ] 5.3 Add example API requests and responses
- [ ] 5.4 Document uvicorn startup command

