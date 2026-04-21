# Recommended Dashboard Scope

Last updated: 2026-04-07

## Purpose

This document defines the recommended product scope for the next phase of the University Leadership Dashboard.

The main product decision is:

- use public data for external strategy, benchmarking, and reputation signals
- use internal university systems for operational management

That means the dashboard should not try to force one data model across very different decision layers.

## Product Positioning

The dashboard should be split into two layers.

### 1. External Strategy Layer

This layer answers:

- Is the student market shrinking or shifting?
- How does Bulgaria compare with the EU and peers?
- What do graduate outcomes, mobility, research output, and R&D conditions look like?
- How visible is the institution in research and EU-funded projects?
- What is the current quality-assurance and accreditation context?

This layer can be built now with public APIs.

### 2. Internal Management Layer

This layer answers:

- How is the admissions funnel performing?
- Where are retention losses happening?
- Which courses create progression friction?
- Which programmes are financially sustainable?
- Where do staffing and teaching-load constraints sit?

This layer requires internal admissions, SIS, LMS, HR, and finance systems.

## Recommended Scope

### Public-Data MVP

Build the next dashboard phase around five pages:

1. Overview
2. Market and Demand
3. Outcomes and Mobility
4. Research and Innovation
5. Quality and Benchmarking

Do not include admissions, retention, course friction, or programme economics in the MVP until internal integrations exist.

## Page Design

### Overview

Purpose:
Give leadership a fast summary of external demand, outcomes, research visibility, EU project activity, and quality status.

Recommended KPI cards:

- Population aged 18-24 in Bulgaria
- Tertiary attainment age 25-34 in Bulgaria vs EU
- Recent graduate employment rate
- International tertiary students share or count
- Researchers FTE or R&D expenditure
- Institution publications count
- Institution citations / h-index
- Active or recent EU project count
- Latest DEQAR institutional QA status

Recommended panels:

- Bulgaria vs EU benchmark snapshot
- Five-year trend panel for demand and outcomes
- Research and EU-funding summary

Important product rule:
Label country/system indicators clearly as national context, not institutional performance.

### Market and Demand

Purpose:
Track the size and direction of the addressable student market.

Recommended views:

- Population aged 18-24 trend
- Population aged 25-34 trend
- Tertiary enrolment benchmark by country
- New entrants to tertiary education by field
- Graduates by field
- International student trend

Recommended questions:

- Is the domestic undergraduate market shrinking?
- Which fields are growing or weakening?
- Is Bulgaria losing or gaining attractiveness relative to peers?

### Outcomes and Mobility

Purpose:
Show whether higher education is translating into employability and international relevance.

Recommended views:

- Tertiary attainment age 25-34
- Employment rate of tertiary-educated adults
- Unemployment rate of tertiary-educated adults
- Recent graduate employment rate
- International tertiary students
- Optional outbound/inbound mobility extensions later

Recommended questions:

- Are labour-market outcomes improving?
- Is Bulgaria converging toward or diverging from EU peers?
- Is internationalisation growing fast enough?

### Research and Innovation

Purpose:
Show the institution's research footprint and the external innovation environment.

Recommended views:

- Institution publications by year
- Citations by year
- h-index and i10 index
- Open-access share
- R&D expenditure as percent of GDP
- Researchers FTE
- EU project participation and net EU contribution
- Research collaboration network from CORDIS later if needed

Recommended questions:

- Is research output growing?
- Is impact growing or only volume?
- Is the university visible in Horizon and related EU programmes?
- Is the national R&D environment supportive enough?

### Quality and Benchmarking

Purpose:
Track institutional credibility and external benchmarking context.

Recommended views:

- DEQAR institutional reports
- DEQAR programme reports if available and relevant
- Accredited / reviewed status summary
- Benchmark table against selected peer universities when institution-level data is available
- Annual benchmark data from EHESO / ETER as a later addition

Recommended questions:

- What is the current QA status of the institution and its programmes?
- Which peer institutions should be used for annual external benchmarking?

## Source Strategy

### Sources approved for the public-data MVP

#### Eurostat

Use for:

- population and demographics
- tertiary attainment
- tertiary enrolment
- new entrants by field
- graduates by field
- recent graduate employment
- tertiary-educated employment and unemployment
- international tertiary students
- R&D expenditure
- researchers FTE

Confirmed suitability:

- strongest source for country-level strategy and benchmarking
- official live API
- latest confirmed years from direct API testing on 2026-04-07:
  - `demo_pjan`: 2025
  - `edat_lfse_03`: 2024
  - `lfsa_ergaed`: 2024
  - `edat_lfse_24`: 2024
  - `educ_uoe_enrt01`: 2024
  - `educ_uoe_ent01`: 2024
  - `educ_uoe_ent02`: 2024
  - `educ_uoe_grad02`: 2024
  - `educ_uoe_mobs02`: 2024
  - `rd_e_gerdtot`: 2024
  - `rd_p_persocc`: 2024

#### OpenAlex

Use for:

- institution publications
- citations
- h-index
- i10 index
- open-access counts
- yearly research output trends

Confirmed suitability:

- strong source for institution-level research visibility
- basic API access is open
- direct test against Sofia University returned a valid institution record and yearly counts including 2026 output

#### CORDIS

Use for:

- EU project participation
- project partners
- net EU contribution
- project trend counts

Confirmed suitability:

- good source for EU research and innovation participation
- official API exists
- API key required
- documentation UI reachable
- live project pages show current updates

#### DEQAR

Use for:

- institutional QA reports
- programme QA reports
- agency and country QA context

Confirmed suitability:

- useful for quality and accreditation context
- official web API exists
- free but registered access is required
- direct endpoint test returned `403` without credentials, which confirms gated access

### Optional later source

#### EHESO / ETER

Use for:

- annual institution benchmarking
- students, graduates, staffing, and finance comparisons across European HEIs

Recommended role:

- phase 2 annual benchmark source
- not a live operational source

Reason:

- important, but lagged
- API access exists, but unrestricted use requires authenticated access
- current evidence supports using it for yearly benchmarking, not for dashboard sections that imply freshness

## Out of Scope Until Internal Data Exists

Do not build these pages as if they were complete:

- Admissions Funnel
- Retention and Dropout
- Course Friction / Progression Bottlenecks
- Programme Economics
- Teaching Load and Capacity

These can appear later, but only after internal systems are connected.

Minimum internal integrations required:

- admissions / CRM
- student information system
- learning management system
- HR / staffing system
- finance / ERP

## MVP Navigation

Recommended top navigation:

- Overview
- Market
- Outcomes
- Research
- Quality

Recommended global controls:

- comparison countries
- time range
- peer university selector for institution-level pages
- export

## Product Guardrails

- Never present national Eurostat metrics as if they were direct university performance measures.
- Separate country-level indicators from institution-level indicators visually and textually.
- Tag each chart with a source label and last available year.
- Use narrative summaries only when the comparison is meaningful.
- Avoid misleading comparisons between Bulgaria and EU aggregates for absolute-size indicators without normalisation.
- Treat EHESO / ETER as annual benchmark context, not live monitoring.
- Treat DEQAR and CORDIS as credentialed integrations during implementation planning.

## Delivery Plan

### Phase 2A: Public-Data MVP

Build now:

- Overview
- Market and Demand
- Outcomes and Mobility
- Research and Innovation
- Quality and Benchmarking

Dependencies:

- Eurostat
- OpenAlex
- CORDIS credentials
- DEQAR credentials

### Phase 2B: Institution Benchmarking

Add later:

- EHESO / ETER annual comparisons
- peer university comparison tables
- institution benchmark narratives

### Phase 3: Internal Management

Add only after internal systems are available:

- admissions funnel
- retention
- course friction
- programme economics
- staffing and capacity

## Recommended Build Priority

1. Market and Demand
2. Outcomes and Mobility
3. Research and Innovation
4. Quality and Benchmarking
5. Overview as the executive synthesis layer

Reason:
the first four pages have the clearest public-source foundations, while the Overview page should be built last so it reflects the final information architecture rather than guessing it too early.
