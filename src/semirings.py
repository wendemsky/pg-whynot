# src/semirings.py
#
# Four semiring implementations used for provenance tracking.
#
# A semiring K = (K, +, ×, 0, 1) must satisfy:
#   - (K, +, 0) is a commutative monoid
#   - (K, ×, 1) is a monoid
#   - × distributes over +
#   - 0 × a = a × 0 = 0  (zero annihilates)
#
# In the K-relations framework (Green et al. 2007):
#   - UNION / OR  maps to  +
#   - JOIN / AND  maps to  ×
#   - Filter pass maps to  × 1 (unchanged)
#   - Filter fail maps to  × 0 (annihilated → missing)
#
# The four semirings differ in how expressive the provenance annotation is:
#
#   Boolean  ⊑  Bag  ⊑  Why  ⊑  How   (information order)
#
# Each semiring is a self-contained class with:
#   zero()        → the additive identity (annihilator for ×)
#   one()         → the multiplicative identity
#   add(a, b)     → semiring addition (UNION / OR)
#   mul(a, b)     → semiring multiplication (JOIN / AND)
#   token(name)   → provenance element for a single base tuple
#   is_zero(a)    → True if a equals the zero element
#   display(a)    → human-readable string for an annotation

from abc import ABC, abstractmethod
from typing import Any, FrozenSet, Dict


# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------

class Semiring(ABC):
    """Abstract base class for all semirings."""

    NAME: str = "abstract"

    @abstractmethod
    def zero(self) -> Any:
        """Additive identity; also the multiplicative annihilator."""

    @abstractmethod
    def one(self) -> Any:
        """Multiplicative identity."""

    @abstractmethod
    def add(self, a: Any, b: Any) -> Any:
        """Semiring addition — used for UNION and projecting duplicate tuples."""

    @abstractmethod
    def mul(self, a: Any, b: Any) -> Any:
        """Semiring multiplication — used for JOIN."""

    @abstractmethod
    def token(self, name: str) -> Any:
        """Create a provenance annotation for a single base tuple.

        Args:
            name: Unique identifier for the base tuple, e.g. 'items_1'.
        """

    def is_zero(self, a: Any) -> bool:
        """True if annotation a is the zero element (tuple was annihilated)."""
        return a == self.zero()

    @abstractmethod
    def display(self, a: Any) -> str:
        """Human-readable string for annotation a."""

    def __repr__(self):
        return f"Semiring({self.NAME})"


# ---------------------------------------------------------------------------
# 1. Boolean semiring  B = ({0, 1}, ∨, ∧, 0, 1)
# ---------------------------------------------------------------------------

class BooleanSemiring(Semiring):
    """
    The coarsest semiring — answers only 'is this tuple in the output?'

    Annotations are True (present) or False (absent).
    Every base tuple gets annotation True.

    Why-not power: Can only confirm a tuple is missing. Gives no reason.
    """

    NAME = "boolean"

    def zero(self) -> bool:
        return False

    def one(self) -> bool:
        return True

    def add(self, a: bool, b: bool) -> bool:
        return a or b

    def mul(self, a: bool, b: bool) -> bool:
        return a and b

    def token(self, name: str) -> bool:
        # Every base tuple is simply 'present'
        return True

    def display(self, a: bool) -> str:
        return "present" if a else "ABSENT"


# ---------------------------------------------------------------------------
# 2. Bag semiring  N = (ℕ, +, ×, 0, 1)
# ---------------------------------------------------------------------------

class BagSemiring(Semiring):
    """
    Tracks multiplicity — how many times a tuple appears in the output.

    Annotations are non-negative integers.
    Each base tuple has multiplicity 1.

    Why-not power: A missing tuple has multiplicity 0.
    Tells us the tuple would appear 0 times, but not WHY.
    Distinguishes 'never matched' from 'matched but filtered'.
    """

    NAME = "bag"

    def zero(self) -> int:
        return 0

    def one(self) -> int:
        return 1

    def add(self, a: int, b: int) -> int:
        return a + b

    def mul(self, a: int, b: int) -> int:
        return a * b

    def token(self, name: str) -> int:
        return 1

    def display(self, a: int) -> str:
        if a == 0:
            return "multiplicity=0 (ABSENT)"
        return f"multiplicity={a}"


# ---------------------------------------------------------------------------
# 3. Why-provenance semiring  W = (2^(2^T), ∪, ⊗, ∅, {∅})
# ---------------------------------------------------------------------------

class WhyProvenance(Semiring):
    """
    Tracks WHICH sets of base tuples are witnesses for the output tuple.

    An annotation is a set of 'witnesses', where each witness is a frozenset
    of base-tuple tokens that together produce this output tuple.

    Example: x_a·x_b + x_c means two witnesses: {a,b} and {c}.

    Operations:
      add(A, B)  = A ∪ B       (union of witness sets)
      mul(A, B)  = {s₁ ∪ s₂ | s₁ ∈ A, s₂ ∈ B}   (pairwise union)

    Why-not power: Missing tuple has annotation ∅ (no witnesses).
    Tells us which base tuples WOULD have been witnesses if they existed
    or had passed the filter — pointing exactly to the root cause.
    """

    NAME = "why"

    # Type alias: annotation is frozenset of frozenset of str
    # Each inner frozenset = one witness (a set of base tuple token names)

    def zero(self) -> FrozenSet:
        return frozenset()  # no witnesses

    def one(self) -> FrozenSet:
        # One witness: the empty set (no base tuples needed — unit of multiplication)
        return frozenset({frozenset()})

    def add(self, a: FrozenSet, b: FrozenSet) -> FrozenSet:
        # Union of witness sets
        return a | b

    def mul(self, a: FrozenSet, b: FrozenSet) -> FrozenSet:
        # Cross-product: each witness from a paired with each witness from b
        if not a or not b:
            return frozenset()  # anything × ∅ = ∅
        return frozenset(
            s1 | s2
            for s1 in a
            for s2 in b
        )

    def token(self, name: str) -> FrozenSet:
        # Single witness containing just this base tuple
        return frozenset({frozenset({name})})

    def display(self, a: FrozenSet) -> str:
        if not a:
            return "witnesses=empty (ABSENT)"
        witnesses = ["{" + ", ".join(sorted(w)) + "}" for w in sorted(a, key=lambda x: sorted(x))]
        return "witnesses={" + ", ".join(witnesses) + "}"


# ---------------------------------------------------------------------------
# 4. How-provenance (polynomial) semiring  ℕ[X]
# ---------------------------------------------------------------------------

class HowProvenance(Semiring):
    """
    The most expressive semiring — tracks the full provenance polynomial.

    An annotation is a polynomial over base-tuple variables, represented as
    a dict mapping each monomial (frozenset of token names) to its coefficient.

    Example: x_items_1 · x_stocks_301_1 + x_items_2 · x_stocks_281_2
      = { frozenset({'items_1', 'stocks_301_1'}): 1,
          frozenset({'items_2', 'stocks_281_2'}): 1 }

    Operations:
      add(P, Q)  = merge polynomials, summing coefficients of equal monomials
      mul(P, Q)  = distribute: each monomial in P × each monomial in Q

    Why-not power: Missing tuple has annotation {} (zero polynomial).
    The polynomial reveals EVERY path through the query that could have
    produced the tuple, and WHY each path failed (each monomial that was
    never formed). This is the most detailed explanation.

    Note: HowProvenance subsumes all other semirings:
      - Collapse to Boolean by mapping every coeff → True
      - Collapse to Bag by summing all coefficients
      - Collapse to Why by dropping coefficients and keeping monomials as witness sets
    """

    NAME = "how"

    # Type alias: dict[frozenset[str], int]
    # Keys are monomials (frozensets of token strings), values are integer coefficients.

    def zero(self) -> Dict:
        return {}  # zero polynomial

    def one(self) -> Dict:
        # The empty monomial with coefficient 1 — the multiplicative identity
        return {frozenset(): 1}

    def add(self, a: Dict, b: Dict) -> Dict:
        result = dict(a)
        for monomial, coeff in b.items():
            result[monomial] = result.get(monomial, 0) + coeff
        return result

    def mul(self, a: Dict, b: Dict) -> Dict:
        if not a or not b:
            return {}  # annihilation: 0 × anything = 0
        result = {}
        for m1, c1 in a.items():
            for m2, c2 in b.items():
                m = m1 | m2
                result[m] = result.get(m, 0) + c1 * c2
        return result

    def token(self, name: str) -> Dict:
        # Single monomial: just the variable for this base tuple, coefficient 1
        return {frozenset({name}): 1}

    def is_zero(self, a: Dict) -> bool:
        return len(a) == 0

    def display(self, a: Dict) -> str:
        if not a:
            return "polynomial=0 (ABSENT)"
        terms = []
        for monomial, coeff in sorted(a.items(), key=lambda x: sorted(x[0])):
            vars_str = " * ".join(sorted(monomial)) if monomial else "1"
            if coeff == 1:
                terms.append(vars_str)
            else:
                terms.append(f"{coeff}*{vars_str}")
        return "polynomial=( " + " + ".join(terms) + " )"


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

SEMIRINGS = {
    "boolean": BooleanSemiring(),
    "bag":     BagSemiring(),
    "why":     WhyProvenance(),
    "how":     HowProvenance(),
}


def get_semiring(name: str) -> Semiring:
    """Look up a semiring by name. Raises ValueError for unknown names."""
    if name not in SEMIRINGS:
        raise ValueError(
            f"Unknown semiring '{name}'. "
            f"Available: {list(SEMIRINGS.keys())}"
        )
    return SEMIRINGS[name]
