"""
Mainboard Optimization Prompt Template

This module contains the LLM prompt template for optimizing mainboard flex spots
based on the current meta archetypes.
"""

MAINBOARD_OPTIMIZATION_PROMPT_TEMPLATE = """You are an expert Magic: The Gathering deck builder analyzing a competitive deck for mainboard optimization in {format} format.

# Your Deck
Archetype: {archetype}
{deck_summary}

# Current Meta Context
You are optimizing against the top {top_n} most frequent archetypes in {format}:

{top_n_archetype_decklists}

# Available Card Pool
Below is a curated list of cards for you to choose from. This list has been filtered to:
- Cards legal in {format}
- Cards that have appeared in tournament decklists in the recent past
- Cards matching your deck's color identity (including colorless)
- Limited to <500 most relevant cards

{available_card_pool}

# Task
Analyze the mainboard for flex spots (cards that are not critical to the deck's core strategy) and recommend replacements that improve performance against the current meta.

## Requirements
1. Only suggest cards from the available card pool above
2. Identify flex spots (cards that can be replaced without breaking the deck's core synergies). This is a subjective judgement and should be based on the actual cards you see in the top archetypes' decklists.
3. For each flex spot, recommend 1-3 replacement cards
4. Provide detailed justifications based on the actual cards you see in the top archetypes' decklists
5. Explain why you did NOT recommend other common alternatives

## Output Format
Return your analysis as valid JSON with the following structure:

{{
  "flex_spots": [
    {{
      "card_name": "Current Card Name",
      "quantity": 2,
      "reason": "Why this card is flexible and not core to strategy"
    }}
  ],
  "recommendations": [
    {{
      "flex_spot_card": "Current Card Name",
      "suggested_cards": [
        {{
          "card_name": "Suggested Replacement",
          "quantity": 2,
          "justification": "Why this card improves against top archetypes. Reference specific cards from the meta decklists.",
          "matchup_improvements": ["Archetype 1 name", "Archetype 2 name"]
        }}
      ],
      "alternatives_considered": [
        {{
          "card_name": "Alternative Card",
          "why_not_recommended": "Specific reason based on meta analysis"
        }}
      ]
    }}
  ]
}}

Be specific and reference actual cards you observe in the top archetype decklists. Only suggest cards that appear in the available card pool provided."""

