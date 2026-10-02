# DESIGN.md — SettleLedger dashboard

Ground truth from the built world (`gateway/internal/dash/web/dashboard.html`),
pinned to the Expo analysis in `DESIGN-expo.md` and adapted so the hero artifact
is the running console rather than a device mockup.

## World in one line
Pure-white canvas, near-black ink, one soft sky wash behind the hero, black as
the only fill, hairline dividers instead of stacked cards, dark panels reserved
for data, Inter for text and JetBrains Mono for every figure and code surface.

## Tokens (as built)
```
canvas #ffffff   canvas-soft #fafafa   card #ffffff    strong #f0f0f3
ink    #171717   body #60646c          muted #8a8f98
link   #0d74ce   dark #171717          dark-elev #1a1a1a
success #16a34a  warning #ab6400       error #eb8e90
hairline #f0f0f3  hairline-soft #f5f5f7  hairline-strong #dcdee0
wash    #cfe7ff → #dcecff → transparent (hero only)
sans 'Inter', -apple-system, system-ui, sans-serif
mono 'JetBrains Mono', ui-monospace, Menlo, Consolas, monospace
radius xs 4 · sm 6 · md 8 · lg 12 · xl 16 · pill 9999
```
Two documented departures from the source analysis, both for contrast:
muted is `#8a8f98` (the source `#999999` is 2.9:1 on white) and placeholders are
`#6b7078` (4.6:1). Everything else follows the brief verbatim.

## Type scale (as built)
display 36/1.15/-1.08px (28px below 1024) · section head 22/1.25/-0.5px ·
card title 18/1.4 · body 16/1.5 · small 14/1.5 · caption 13/1.4 ·
caption-uppercase 11/1.4/+0.88px · code 13/1.5 mono · button 14/1.0.
Weights stay in the 400–600 band; display never drops below 600.

## Layout rhythm
1200px content width. Hero 72px top padding, 460px wash, headline max 18ch,
subhead max 60ch. Bands separated by 48–64px. Three lanes collapse 3-up → 1-up
at 900px; exception lists 3-up → 1-up at 760px; footer 3-column → 1-column at
760px; nav links hidden below 860px with the CTA kept.

## Components
- **nav** 64px, white, hairline bottom, wordmark left, four links, black CTA.
- **btn** black fill, 8px radius, 40px (34px small); secondary is a white card
  with a strong hairline; pressed state `#1a1a1a`; focus ring is link blue.
- **pill** status only (mode indicator), never a CTA.
- **console** 16px radius, hairline, one soft shadow, the page's chrome.
- **chip** 30px pill; tinted surfaces carry state (success/warning/error) with
  the figure in the same hue; quiet chips are canvas-soft with a hairline.
- **lane** 12px card with a 4px ink meter that scales on `transform`.
- **dark card** 12px inversion used only for data surfaces (bench panel,
  JSON, saga detail, code).
- **step** 32px square plate in `--strong` with a mono number.
- **bar row** 8px track, white fill on dark, label and value in mono.

## Rules this surface keeps
Black is the only fill. Link blue never appears on a button. Pills are for
status only. Every meter and bar animates on `transform`, never `width`.
Motion is one authored moment (data arriving) plus bar growth, both gated by
`prefers-reduced-motion`. Numbers are mono; prose is Inter. No icon font: the
three marks are authored inline SVG at one stroke weight.

## Known departures from the source analysis
The device-mockup hero becomes the live console (the artifact leads rather than
a photograph of it). Dark inversion is used for data panels instead of marketing
cards. Fonts are referenced by stack, not self-hosted, until the repo carries a
webfont directory.