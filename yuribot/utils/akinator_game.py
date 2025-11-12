"""Light-weight akinator style game logic used by the slash command."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from ..data.akinator_sets import (
    AKINATOR_SETS,
    DEFAULT_SET,
    YURI_SET,
    CharacterEntry,
    GameSet,
)


AnswerKey = str

ANSWER_VALUES: Dict[AnswerKey, float] = {
    "yes": 1.0,
    "probably": 0.5,
    "unknown": 0.0,
    "probably_not": -0.5,
    "no": -1.0,
}


MATCH_TABLE: Dict[AnswerKey, Sequence[AnswerKey]] = {
    "yes": ("yes", "probably"),
    "probably": ("yes", "probably", "unknown"),
    "unknown": tuple(ANSWER_VALUES.keys()),
    "probably_not": ("no", "probably_not", "unknown"),
    "no": ("no", "probably_not"),
}


YES_BUCKET = {"yes", "probably"}
NO_BUCKET = {"no", "probably_not"}


@dataclass
class GuessResult:
    character: CharacterEntry
    confidence: float


class AkinatorGame:
    """A tiny deterministic akinator implementation built on curated data."""

    def __init__(self, *, yuri_mode: bool = False) -> None:
        key = YURI_SET if yuri_mode else DEFAULT_SET
        dataset = AKINATOR_SETS[key]
        self.dataset_key = key
        self.dataset: GameSet = dataset
        self.questions: Sequence[str] = dataset["questions"]
        self.characters: Sequence[CharacterEntry] = dataset["characters"]
        self._remaining_questions: set[int] = set(range(len(self.questions)))
        self._history: List[Tuple[int, AnswerKey]] = []
        self._candidates: List[int] = list(range(len(self.characters)))
        self._current_question: Optional[int] = self._choose_next_question()

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------
    @property
    def title(self) -> str:
        return self.dataset["title"]

    @property
    def question_number(self) -> int:
        return len(self._history) + 1

    def current_question(self) -> Optional[str]:
        if self._current_question is None:
            return None
        return self.questions[self._current_question]

    def record_answer(self, answer: AnswerKey) -> None:
        if self._current_question is None:
            return
        normalized = answer if answer in ANSWER_VALUES else "unknown"
        q_idx = self._current_question
        self._history.append((q_idx, normalized))
        self._remaining_questions.discard(q_idx)
        self._apply_answer_filter(q_idx, normalized)
        self._current_question = self._choose_next_question()

    def should_guess(self) -> bool:
        return (
            self._current_question is None
            or len(self._candidates) <= 1
            or len(self._history) >= len(self.questions)
        )

    def best_guess(self) -> Optional[GuessResult]:
        if not self._candidates:
            return None
        best_idx = max(
            self._candidates,
            key=lambda idx: self._score_candidate(idx),
        )
        character = self.characters[best_idx]
        confidence = self._score_candidate(best_idx)
        return GuessResult(character=character, confidence=confidence)

    def top_candidates(self, limit: int = 3) -> List[GuessResult]:
        ranked = sorted(
            (GuessResult(character=self.characters[idx], confidence=self._score_candidate(idx))
             for idx in self._candidates),
            key=lambda r: r.confidence,
            reverse=True,
        )
        return ranked[:limit]

    def candidate_count(self) -> int:
        return len(self._candidates)

    def history(self) -> List[Tuple[int, AnswerKey]]:
        return list(self._history)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _choose_next_question(self) -> Optional[int]:
        if not self._remaining_questions:
            return None
        best_idx: Optional[int] = None
        best_score: Optional[float] = None
        best_total = 0
        for idx in self._remaining_questions:
            yes_like = 0
            no_like = 0
            total = 0
            for c_idx in self._candidates:
                answer = self.characters[c_idx]["answers"][idx]
                if answer in YES_BUCKET:
                    yes_like += 1
                    total += 1
                elif answer in NO_BUCKET:
                    no_like += 1
                    total += 1
            if total == 0:
                continue
            imbalance = abs(yes_like - no_like) / total
            if best_score is None or imbalance < best_score - 1e-6 or (
                abs(imbalance - (best_score or 0.0)) <= 1e-6 and total > best_total
            ):
                best_score = imbalance
                best_total = total
                best_idx = idx
        if best_idx is None:
            best_idx = next(iter(self._remaining_questions))
        return best_idx

    def _apply_answer_filter(self, q_idx: int, answer: AnswerKey) -> None:
        allowed = MATCH_TABLE.get(answer, MATCH_TABLE["unknown"])
        filtered = [
            idx
            for idx in self._candidates
            if self.characters[idx]["answers"][q_idx] in allowed
        ]
        if filtered:
            self._candidates = filtered

    def _score_candidate(self, idx: int) -> float:
        if not self._history:
            return 0.5
        answers = self.characters[idx]["answers"]
        score = 0.0
        for q_idx, user_answer in self._history:
            char_answer = answers[q_idx]
            u_val = ANSWER_VALUES.get(user_answer, 0.0)
            c_val = ANSWER_VALUES.get(char_answer, 0.0)
            score += max(0.0, 1 - abs(u_val - c_val) / 2)
        return score / max(1, len(self._history))


def create_game(*, yuri_mode: bool) -> AkinatorGame:
    return AkinatorGame(yuri_mode=yuri_mode)

