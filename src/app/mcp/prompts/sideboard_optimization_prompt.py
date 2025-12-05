"""
Sideboard Optimization Prompt Template

This module contains the LLM prompt template for optimizing sideboard cards
based on the current meta archetypes and opponent sideboard plans.
"""

SIDEBOARD_OPTIMIZATION_PROMPT_TEMPLATE = """You are an expert Magic: The Gathering deck builder analyzing a competitive sideboard for optimization in {format} format.

# Your Deck
Archetype: {archetype}
{deck_summary}

# Current Sideboard
{current_sideboard}

# Current Meta Context
You are optimizing against the top {top_n} most frequent archetypes in {format}:

{top_n_archetype_decklists}

# Available Card Pool
Below is a curated list of cards for you to choose from. This list has been filtered to:
- Cards legal in {format}
- Cards that have appeared in tournament decklists in the recent past
- Cards matching your deck's color identity (including colorless)
- Limited to ~500 most relevant cards

{available_card_pool}

# Task
Optimize the sideboard to better answer the top {top_n} archetypes, considering:
1. What threats each archetype presents (from mainboard AND sideboard)
2. What opponent sideboard cards you need to answer in games 2 and 3
3. The current sideboard's gaps and redundancies

## Requirements
1. Only suggest cards from the available card pool above
2. The final sideboard MUST contain exactly 15 cards total
3. Recommend specific additions (cards to add) and removals (cards to cut)
4. Justify each change based on actual cards observed in opponent decklists and sideboards
5. Consider post-sideboard games: what do opponents bring in against your archetype?
6. Explain how the sideboard plan changes for games 2 and 3 against each top archetype

## Output Format
Return your analysis as valid JSON with the following structure:

{{
  "sideboard_changes": [
    {{
      "remove": {{
        "card_name": "Card to Remove",
        "quantity": 2,
        "reason": "Why this card is less valuable against current meta"
      }},
      "add": {{
        "card_name": "Card to Add",
        "quantity": 2,
        "justification": "Why this card is needed. Reference specific threats from opponent decklists and sideboards.",
        "answers_archetypes": ["Archetype 1", "Archetype 2"]
      }}
    }}
  ],
  "sideboard_plans": [
    {{
      "opponent_archetype": "Archetype Name",
      "games_2_3_plan": "What to bring in, what to take out, and why. Consider what opponent brings in against you.",
      "key_cards_to_answer": ["Opponent card 1", "Opponent card 2"]
    }}
  ],
  "final_sideboard": [
    {{
      "card_name": "Card Name",
      "quantity": 3
    }}
  ]
}}

CRITICAL: The final_sideboard array must contain exactly 15 cards total (sum of all quantities must equal 15).

Be specific and reference actual cards you observe in opponent mainboards and sideboards. Only suggest cards that appear in the available card pool provided."""

