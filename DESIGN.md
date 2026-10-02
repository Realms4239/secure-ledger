# DESIGN.md — SettleLedger folio

Ground truth from the built world (`gateway/internal/dash/web/dashboard.html`):
a clearing-house ledger folio. Ruled paper, folio stamps and running totals
replace both the SaaS marketing anatomy (rejected as boilerplate) and the
generic dark console. The money is the display type; states arrive as stamps.

## World in one line
Warm paper, ink-black ruling with single and double rules doing all
hierarchy, tabular JetBrains Mono figures, bordered letterspaced stamps for
state, operator monograms instead of hue coding, radius capped at 4px.

## Tokens (as built)
```
paper #fbfaf7   paper-dim #f4f1ea   card #fffdf9
ink #1a1815     body #4a463d        faint #8b8474
rule #e3ddd0    rule-faint #efeadd  rule-strong #1a1815
ok #1e7a46      warn #9a6a1a        bad #a02c2c   link #1d4ed8
sans Inter stack · mono JetBrains Mono stack
radius 2-4px (stamps 2-3px, inputs/buttons 3px, code 4px)
```
Body text never sets below `#4a463d` (7.5:1 on paper); faint gray is reserved
for datelines, captions and empty states. Link blue passes AA on paper.

## Type scale (as built)
Nameplate 30px/+.22em · display numerals 34px mono · section heads 13px
uppercase/+.16em · card titles and body 14-15px/1.55 · captions 12.5px ·
mono figures 12.5px. Weight never leaves 400–700; emphasis is weight or size.

## Layout rhythm
1180px folio width. Masthead with a 3px double rule under; totals strip with
single rule above and double rule below; sections 44px apart, each opened by
an uppercase head and one standfirst line. Operator columns share one ruled
frame with hairline gutters. Mobile: totals 2-up, lanes and lists stack.

## Signature devices
- **Masthead**: nameplate + live/recorded stamp + dateline with folio number
  (Nºyyyymmdd, real date) + one-line deck. No nav, no CTA band.
- **Running totals**: accepted / held / retries / limited as 34px mono
  numerals closed by a double rule — the money is the display type.
- **Postings tape**: every edge request as a ruled line with a stamp
  (posted/held/retry/limited/clear/filed), txid in mono, amount right.
- **Operator columns**: monogram plates (MV/OR/AT), per-column count, a 3px
  ink meter scaling on `transform`, ruled rows, sub-ledger foot with count
  and minor-unit sum.
- **The close**: folio table with right-aligned mono figures, exceptions as
  three ruled lists (missing rows link into lookup), decided-row total under
  a double rule, raw report behind a disclosure.
- **Signal-box block**: lamp + CLEAR/OCCUPIED instrument for the partition
  drill, fused from the dealt direction the roll refused to build whole.
- **Code and lookup panels**: dark ink inversion kept only for machine
  surfaces (raw JSON, saga detail, curl), matching the repo's docs voice.

## Rules this surface keeps
No gradients, no shadows, no cards-with-shadow, no pills (stamps are
bordered rectangles), no saturated fills. Meters and bars animate on
`transform` only. Motion is one authored moment (data arriving), gated by
`prefers-reduced-motion`. Numbers are mono; prose is Inter. Inline SVG only
for the favicon; no icon font. Every figure is measured or labeled.

## Known positions
Serif was considered for the nameplate (ledger heritage) and refused: the
numbers already carry the typographic identity, and a display serif would
fight the mono figures. Dark mode was refused: the paper is the brand.
Photography and illustration were refused: the live ledger is the imagery.
