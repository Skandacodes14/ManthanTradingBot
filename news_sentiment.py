"""
Corporate News Sentiment Engine for MANTHAN 2026 Autonomous Trading Bot.

Processes `market.news` flashes arriving at T+150ms in under 0.05ms per message.
Provides:
- Normalized sentiment score in [-1.0, 1.0] via `score_headline(text)`
- Raw net keyword differential via `raw_keyword_score(text)` (used by Strategy.py
  threshold rules such as raw_score >= 2 or raw_score <= -2)
- Combined headline + description analysis via `analyze_news(headline, description)`
"""

from __future__ import annotations

import re
from typing import Dict, FrozenSet, Union

# Core competition keywords from Beginners Guide & Implementation Plan,
# plus standard corporate earnings/disclosure inflections.
BULLISH_KEYWORDS: FrozenSet[str] = frozenset({
    "PROFIT",
    "PROFITS",
    "SURGE",
    "SURGES",
    "SURGED",
    "GROWTH",
    "RECORD",
    "BEAT",
    "BEATS",
    "EXPANSION",
    "EXPANDS",
    "DIVIDEND",
    "DIVIDENDS",
    "CONTRACT",
    "CONTRACTS",
    "PARTNERSHIP",
    "PARTNERSHIPS",
    "UPGRADE",
    "UPGRADES",
    "UPGRADED",
    "BREAKTHROUGH",
    "ACQUISITION",
    "BUYBACK",
    "RALLY",
    "SOARS",
    "JUMP",
    "JUMPS",
    "STRONG",
    "OUTPERFORM",
    "APPROVAL",
})

BEARISH_KEYWORDS: FrozenSet[str] = frozenset({
    "LOSS",
    "LOSSES",
    "PLUNGE",
    "PLUNGES",
    "PLUNGED",
    "DROP",
    "DROPS",
    "DROPPED",
    "FRAUD",
    "SCRUTINY",
    "RESIGNATION",
    "RESIGNS",
    "PROBE",
    "PROBES",
    "DOWNGRADE",
    "DOWNGRADES",
    "DOWNGRADED",
    "DEFAULT",
    "DEFAULTS",
    "STRIKE",
    "STRIKES",
    "LAWSUIT",
    "PENALTY",
    "FINE",
    "FINED",
    "BANKRUPTCY",
    "INSOLVENCY",
    "RECALL",
    "CRASH",
    "MISS",
    "MISSES",
    "WEAK",
    "DEFICIT",
    "SLUMP",
    "SLUMPS",
    "INVESTIGATION",
})

NEGATION_WORDS: FrozenSet[str] = frozenset({
    "NO",
    "NOT",
    "WITHOUT",
    "DENIES",
    "DENIED",
    "AVOIDS",
    "AVOIDED",
    "FALSE",
})

# Pre-compiled word tokenizer (extracts uppercase alphanumeric tokens in ~5 microseconds)
_WORD_RE = re.compile(r"[A-Z]+")


def count_sentiment_keywords(text: str) -> tuple[int, int]:
    """
    Count bullish and bearish keywords in `text` with single-word negation awareness.

    If a keyword is immediately preceded by a negation token (e.g. "NO FRAUD",
    "NOT PROFITABLE"), its polarity is inverted or neutralized.

    Args:
        text: Headline or news body string.

    Returns:
        tuple[int, int]: (bullish_count, bearish_count)
    """
    if not text or not isinstance(text, str):
        return (0, 0)

    tokens = _WORD_RE.findall(text.upper())
    if not tokens:
        return (0, 0)

    bullish = 0
    bearish = 0

    for i, token in enumerate(tokens):
        negated = i > 0 and tokens[i - 1] in NEGATION_WORDS
        if token in BULLISH_KEYWORDS:
            if negated:
                bearish += 1
            else:
                bullish += 1
        elif token in BEARISH_KEYWORDS:
            if negated:
                # e.g., "NO LOSS" or "DENIES FRAUD" -> neutralizes bearish hit
                continue
            else:
                bearish += 1

    return (bullish, bearish)


def raw_keyword_score(text: str) -> int:
    """
    Return the net integer keyword count (bullish_count - bearish_count).

    Matches Implementation Plan Section 5.7:
    - Strong positive news score >= 2 -> BUY
    - Strong negative news score <= -2 -> SELL / SHORT
    """
    bullish, bearish = count_sentiment_keywords(text)
    return bullish - bearish


def score_headline(text: str) -> float:
    """
    Score a news headline or text snippet onto the continuous interval [-1.0, 1.0].

    Matches Implementation Plan Section 5.6:
    `score_headline(text: str) -> float: Returns score in [-1.0, 1.0].`

    Scaling logic:
    - 0 net keywords -> 0.0 (Neutral)
    - +1 net keyword (and 0 opposing) -> +0.5 (Moderate Bullish)
    - >= +2 net keywords (and 0 opposing) -> +1.0 (Strong Bullish)
    - Mixed keywords -> damped by `(bullish - bearish) / max(bullish + bearish, 2)`
      and clamped to [-1.0, 1.0].

    Args:
        text: Corporate news headline (or combined headline + description).

    Returns:
        float: Normalized sentiment score in [-1.0, 1.0].
    """
    bullish, bearish = count_sentiment_keywords(text)
    total = bullish + bearish
    if total == 0:
        return 0.0

    net = bullish - bearish
    # Use denominator max(total, 2) so a single keyword hit yields +/- 0.5
    # while 2+ unambiguous hits reach full +/- 1.0 conviction.
    score = net / max(total, 2)
    return max(-1.0, min(1.0, float(score)))


def analyze_news(
    headline: str,
    description: str = "",
) -> Dict[str, Union[float, int, str]]:
    """
    Evaluate a complete `news_flash` event payload (headline + description).

    Headlines carry 2x weight relative to description text to prioritize the primary
    announcement while still capturing secondary disclosures (e.g., "declares interim dividend").

    Args:
        headline: The `headline` field from `market.news`.
        description: Optional `description` field from `market.news`.

    Returns:
        dict with keys:
            - `score`: Normalized float in [-1.0, 1.0]
            - `raw_score`: Net integer keyword count across headline + description
            - `bullish_hits`: Total bullish keyword matches
            - `bearish_hits`: Total bearish keyword matches
            - `signal`: "BUY" | "SHORT" | "NEUTRAL"
    """
    h_bull, h_bear = count_sentiment_keywords(headline)
    d_bull, d_bear = count_sentiment_keywords(description)

    weighted_bull = (2.0 * h_bull) + d_bull
    weighted_bear = (2.0 * h_bear) + d_bear
    total_bull = h_bull + d_bull
    total_bear = h_bear + d_bear
    raw_net = total_bull - total_bear

    # Weighted net for normalized [-1.0, 1.0] score (headline weighted 2x)
    weighted_total = weighted_bull + weighted_bear

    if weighted_total == 0.0:
        norm_score = 0.0
    else:
        norm_score = (weighted_bull - weighted_bear) / max(weighted_total, 2.0)
        norm_score = max(-1.0, min(1.0, norm_score))

    if raw_net >= 2 or norm_score >= 0.5:
        signal = "BUY"
    elif raw_net <= -2 or norm_score <= -0.5:
        signal = "SHORT"
    else:
        signal = "NEUTRAL"

    return {
        "score": round(norm_score, 4),
        "raw_score": raw_net,
        "bullish_hits": total_bull,
        "bearish_hits": total_bear,
        "signal": signal,
    }