# Final Report

Two report variants are provided:

- `final_report.tex`: the main submission version in IEEE conference format.
- `template_style_report.tex`: the same paper in the visual style of
  `StudentID_report.tex` (A4, single column, title page, and table of contents).

The IEEE version should be preferred because the instructor explicitly
requested IEEE document format.

Build either report with:

```bash
cd final_report
MPLBACKEND=Agg python3 generate_figures.py
latexmk -pdf final_report.tex
latexmk -pdf template_style_report.tex
```

Generated figures:

- `figures/architecture.pdf`
- `figures/aqi_trends.pdf`
- `figures/model_results.pdf`

The quantitative results are explicitly described as synthetic-data results.
Real-data validity and infrastructure throughput/latency remain future
evaluation work.
