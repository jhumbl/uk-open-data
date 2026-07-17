# Dataset candidates

The working backlog of datasets we could add to this repo, focused on
Westminster / London local-authority needs: highways, parking, public realm,
public protection & licensing, housing, and general place data.

Researched 2026-07-09. URLs were live at that date; anything marked
*(unverified)* was found in secondary sources and needs checking before
building. When a candidate is built, keep its row and mark it ✅ (see
*Status* below) so coverage is visible at a glance; update the name/notes
if the as-built source differs from the proposal.

Ported 2026-07-17 from the v1 repo (where it was written), with status
marks brought up to date and v1-era conventions (archive tiers, CSV/gzip
outputs) rewritten for the current pipeline (HF family repos, raw file +
one tidy parquet per the source contract in CLAUDE.md).

## How to read this list

**Status**: a ✅ prefix on the *Proposed source* cell means the source is
implemented under `sources/` (the cell shows the as-built name).

**Feasibility** (how it would be fetched under the source contract):

| Code | Meaning |
| --- | --- |
| **A** | Direct download — stable or predictable URL, plain HTTP GET |
| **B** | Scrape — stable landing page, scrape the current file link (the VEH0132 pattern) |
| **C** | Key/registration — needs a free API key or account → store as a CI secret |
| **D** | Awkward — no bulk export, licence unclear, or heavy scraping; deprioritise |

**Publisher codes** (extends the examples in CLAUDE.md's naming
convention — first use of a new code should follow these):

| Code | Publisher |
| --- | --- |
| `dft` | Department for Transport |
| `tfl` | Transport for London |
| `gla` | Greater London Authority (London Datastore) |
| `mps` | Metropolitan Police Service (via London Datastore) |
| `police` | Home Office police data (data.police.uk) |
| `mhclg` | Ministry of Housing, Communities & Local Government |
| `defra` | Dept for Environment, Food & Rural Affairs |
| `fsa` | Food Standards Agency |
| `ho` | Home Office |
| `ons` | Office for National Statistics |
| `hmlr` | HM Land Registry |
| `wcc` | Westminster City Council |
| `lonc` | London Councils |
| `gc` | Gambling Commission |
| `iab` | Inside Airbnb |
| `che` | Companies House |
| `cqc` | Care Quality Commission |
| `he` | Historic England |
| `ukpn` | UK Power Networks |
| `ofcom` | Ofcom |
| `osm` | OpenStreetMap (Overpass extracts) |
| `lt` | London Tribunals |
| `tm` | Toilet Map (Public Convenience Ltd) |

## Suggested first wave

Highest value-to-effort, all feasibility A or B, and together they exercise
every part of the pipeline (daily-updating JSON, quarterly ODS, annual CSV,
borough-level filtering):

1. ✅ `fsa_fhrs_london_food_hygiene` — daily-updated, no key, JSON/XML (built all-London, not just Westminster)
2. `dft_rdc0120_road_condition` — the core highways-condition series
3. `lonc_pcn_enforcement_stats` — London parking enforcement, borough level
4. `dft_evci_charging_devices` — EV charging devices by LA, quarterly
5. `mhclg_homelessness_detailed_la` — statutory homelessness, quarterly
6. `defra_flytipping_la` — fly-tipping incidents & enforcement, annual
7. `mps_crime_dashboard` — Met crime by borough, monthly *(the crime slot
   was instead filled by ✅ `police_crime_london`, full incident-level
   history; the dashboard remains a candidate as a lighter ward-level
   complement)*
8. ✅ `gla_public_realm_trees` — 1.1m London street/park trees (built all-London, November 2025 edition)
9. `wcc_air_quality` — Westminster's own monitor data, CSV
10. `dft_stats19_collisions_london` — road casualties, annual, filter to London

---

## 1. Highways & traffic

| Proposed source | Dataset | Cadence | Format | Feas. | Notes |
| --- | --- | --- | --- | --- | --- |
| `dft_rdc0120_road_condition` | [Road condition (RDC) tables](https://www.gov.uk/government/statistical-data-sets/road-condition-statistics-data-tables-rdc) — % of LA roads red/amber/green, by LA | Annual | ODS | B | RDC0120–0123. Same gov.uk landing-page pattern as VEH0132. Small files (~50 KB) |
| `dft_rdl0201_road_lengths` | [Road lengths (RDL)](https://www.gov.uk/government/statistical-data-sets/road-length-statistics-rdl) by LA | Annual | ODS | B | Denominator for lots of per-km metrics |
| `dft_tra8901_la_traffic` | [Traffic estimates by LA](https://roadtraffic.dft.gov.uk/downloads) — vehicle miles per LA, 1993→ | Annual | CSV (zip) | A/B | roadtraffic.dft.gov.uk hosts direct CSVs (backed by storage.googleapis.com/dft-statistics). LA-level file is small |
| `dft_aadf_count_points_london` | [AADF traffic counts](https://roadtraffic.dft.gov.uk/downloads) at count-point level | Annual | CSV (zip) | B | GB-wide file is large — tidy step should filter to London boroughs into the parquet |
| `dft_stats19_collisions_london` | [STATS19 road casualty data](https://www.gov.uk/government/collections/road-accidents-and-safety-statistics) — collisions/casualties/vehicles | Annual (Sept) | CSV | A | Predictable URLs: `data.dft.gov.uk/road-accidents-safety-data/dft-road-casualty-statistics-{collision,casualty,vehicle}-{year}.csv`. Filter to London police districts in the tidy parquet |
| ✅ `dft_veh0105_licensed_by_la` | [Licensed vehicles by LA](https://www.gov.uk/government/statistical-data-sets/vehicle-licensing-statistics-data-tables) (companion to our VEH0132) | Quarterly | ODS | B | Built 2026-07: same landing-page scrape as VEH0132, shares the `dft-vehicle-licensing` family |
| `dft_evci_charging_devices` | [EV public charging infrastructure by LA](https://www.gov.uk/government/collections/electric-vehicle-charging-infrastructure-statistics) | Quarterly | ODS | B | Note: series re-based in 2026 from "devices" to "chargers"; historical device series continues as table EVCI9001 |
| `dft_street_manager_permits` | [Street Manager open data](https://department-for-transport-streetmanager.github.io/street-manager-docs/open-data/) — every street/road works permit in England | Continuous | API (SNS/JSON) | C | Pub/sub model needs registration + an endpoint — doesn't fit our weekly-pull model directly. Check whether DfT's [Find Transport Data](https://findtransportdata.dft.gov.uk/dataset/roadworks-service-api-street-manager) offers periodic extracts before building |
| `tfl_roads_data` | [TfL roads open data bucket](https://roads.data.tfl.gov.uk/) — assets, traffic monitoring | Varies | CSV etc. | A | Open S3 bucket with index; needs a browse to pick specific high-value files |

## 2. Parking & kerbside

| Proposed source | Dataset | Cadence | Format | Feas. | Notes |
| --- | --- | --- | --- | --- | --- |
| `lonc_pcn_enforcement_stats` | [London Councils enforcement & appeals statistics](https://www.londoncouncils.gov.uk/services/parking-services/enforcement-and-appeals-statistics) — PCNs issued/paid/appealed per borough incl. Westminster, parking + bus lane + moving traffic | Annual | XLSX | B | ~9.4m PCNs across London in 2024-25. Last 5 years on the page |
| `wcc_parking_bays` | Westminster parking bay/space inventory | Static? | CSV/GeoJSON | D | Only found a legacy copy on [Camden's Socrata portal](https://opendata.camden.gov.uk/dataset/Westminster-Parking-Spaces/2579-98vt) (has a CSV export API but likely stale). Worth asking parking services for the current canonical extract — the council itself is the publisher here |
| `wcc_cpz_zones` | Westminster controlled parking zones | Static | PDF maps | D | Published as PDF maps only — not machine-readable. Candidate for an internal GIS export instead |
| — | Parking occupancy / ParkRight live bay sensors | Live | — | D | No public bulk feed found; historic LGA case-study only |

## 3. Public realm & environment

| Proposed source | Dataset | Cadence | Format | Feas. | Notes |
| --- | --- | --- | --- | --- | --- |
| ✅ `gla_public_realm_trees` | [London public realm trees](https://data.london.gov.uk/dataset/local-authority-maintained-trees/) — ~1.14m trees, species + location | Occasional | CSV | A | Built 2026-07: fetches the newest CSV resource via the Datastore JSON API (each edition is a new resource/UUID, so no hard-coded URL). Kept all-London, no Westminster filter; size columns stay strings (suppliers mix exact values and bands). Family `gla-trees` |
| `wcc_air_quality` | [Westminster air quality data download](https://www.westminster.gov.uk/about-council/data/air-quality-data) — monitor + diffusion-tube data | Hourly/monthly | CSV | B | Council's own publication; need to inspect the download page for URL stability |
| `gla_air_quality_stats` | [Air quality summary statistics](https://data.london.gov.uk/dataset/air-quality-summary-statistics) / [monitoring sites](https://data.london.gov.uk/dataset/air_quality_monitoring_sites) | Annual/occasional | CSV | A | Whether sites met objectives, by borough |
| `laqn_westminster_monitors` | [London Air Quality Network API](https://www.londonair.org.uk/Londonair/API/) — hourly readings for Westminster sites | Hourly | JSON/CSV | A | No key needed. Weekly pull of last-week hourly data per WM site; would grow — consider monthly aggregation |
| `defra_flytipping_la` | [Fly-tipping incidents & enforcement by LA](https://www.gov.uk/government/statistics/fly-tipping-statistics-for-england) | Annual | CSV/ODS | B | Data back to 2012-13; hosted on data.defra.gov.uk S3 |
| `defra_la_collected_waste` | [LA collected waste / recycling rates](https://www.gov.uk/government/statistical-data-sets/env18-local-authority-collected-waste-annual-results-tables) (ENV18) | Annual | ODS | B | Recycling rate per borough *(URL unverified — check the exact statistical data set page)* |

## 4. Public protection & licensing

| Proposed source | Dataset | Cadence | Format | Feas. | Notes |
| --- | --- | --- | --- | --- | --- |
| ✅ `fsa_fhrs_london_food_hygiene` | [FSA food hygiene ratings](https://ratings.food.gov.uk/open-data) — every rated food business in London (all 33 LAs, not just Westminster) | Daily | XML + [JSON API](https://api.ratings.food.gov.uk/help) | A | Built 2026-07-09: merged parquet + one zip of the 33 raw per-LA XMLs, ~81k establishments. Store full data, filter in the app |
| `ho_alcohol_licensing_la` | [Alcohol & late night refreshment licensing](https://www.gov.uk/government/collections/alcohol-and-late-night-refreshment-licensing-england-and-wales-statistics) — premises licences, TENs, 24-hour licences by licensing authority | Annual | ODS | B | LA breakdowns back to 2012. Westminster is the biggest licensing authority in the country — high local interest |
| `gc_gambling_premises` | Gambling Commission premises licence register | Ongoing | CSV | B | *(unverified)* Commission publishes register extracts; confirm bulk-download URL and licence |
| — | Westminster premises licence register | Live | Web register | D | [licensing.westminster.gov.uk](https://licensing.westminster.gov.uk/sf/control/publicregister) is search-only, no bulk export. An internal extract or FOI would be needed — flag to PPL colleagues |
| `wcc_property_licensing` | [Westminster selective/HMO property licence register](https://westminster.metastreet.co.uk/public-register) | Ongoing | Web register | D | Metastreet portal; check if it exposes a CSV export before writing off |

## 5. Housing & planning

| Proposed source | Dataset | Cadence | Format | Feas. | Notes |
| --- | --- | --- | --- | --- | --- |
| `mhclg_homelessness_detailed_la` | [Statutory homelessness detailed LA tables](https://www.gov.uk/government/statistical-data-sets/live-tables-on-homelessness) | Quarterly | ODS | B | H-CLIC based; includes temporary accommodation — a huge Westminster pressure |
| `mhclg_rough_sleeping` | [Rough sleeping snapshot](https://www.gov.uk/government/statistics/annual-statistics-on-rough-sleeping-in-england) by LA | Annual | ODS | B | Westminster consistently has England's highest count |
| `mhclg_affordable_housing_supply` | [Affordable housing supply live tables](https://www.gov.uk/government/statistical-data-sets/live-tables-on-affordable-housing-supply) (LT1011+) | Annual | ODS | B | Completions/starts by tenure and LA |
| `mhclg_dwelling_stock` | [Dwelling stock + vacants live tables](https://www.gov.uk/government/statistical-data-sets/live-tables-on-dwelling-stock-including-vacants) (LT100/615) | Annual | ODS | B | Includes long-term empty homes by LA |
| `mhclg_council_taxbase` | [Council taxbase (CTB) statistics](https://www.gov.uk/government/collections/council-taxbase-statistics) | Annual | ODS | B | Dwellings by band, discounts, second homes — by LA |
| `epc_domestic_westminster` | [Energy Performance Certificates by LA](https://epc.opendatacommunities.org/) | Monthly | CSV (zip) | C | Free registration → API key as CI secret; per-LA files have permanent predictable URLs. NB service moving to "Get energy performance of buildings data" — old site retires 30 May 2026, build against the new one |
| `hmlr_price_paid_westminster` | [HMLR Price Paid Data](https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads) — every residential sale | Monthly (20th working day) | CSV | A | Full file is 5.3 GB — fetch the *monthly update* file (or current-year file) and filter to Westminster in the tidy parquet |
| `hmlr_ukhpi_london` | [UK House Price Index](https://landregistry.data.gov.uk/app/ukhpi/) | Monthly | CSV | A | Small, per-LA indices incl. Westminster |
| `ons_private_rents` | [Price Index of Private Rents (PIPR)](https://www.ons.gov.uk/economy/inflationandpriceindices/datasets/priceindexofprivaterentsukmonthlypricestatistics) by LA | Monthly | XLSX/CSV | B | *(exact dataset URL to confirm — ONS datasets have stable landing pages with versioned files)* |
| `gla_pld_applications` | [Planning London Datahub](https://data.london.gov.uk/dataset/planning-london-datahub) — all London planning applications | Daily | Elasticsearch API (JSON, guest read) | B/C | Guest read-only access documented ([API guide](https://www.london.gov.uk/sites/default/files/planninglondondatahub_api_connection_technical_documentation_v1.pdf)). Weekly pull of Westminster applications → parquet. Replaces the London Development Database |

## 6. People, crime & general place data

| Proposed source | Dataset | Cadence | Format | Feas. | Notes |
| --- | --- | --- | --- | --- | --- |
| `mps_crime_dashboard` | [MPS monthly crime dashboard data](https://data.london.gov.uk/dataset/mps-monthly-crime-dashboard-data-e5n6w) — offences by borough/ward | Monthly | CSV | A | Much lighter than data.police.uk bulk archive; borough + ward level. Still a candidate as a complement to the built `police_crime_london` |
| ✅ `police_crime_london` | [data.police.uk street-level crime](https://data.police.uk/data/) | Monthly | CSV (zip) | B | Built 2026-07, bigger than proposed: full all-London history (Met + City of London, Dec 2010→) reconstructed from the permanent monthly archive zips — crimes, outcomes and stop & search parquets; no raw zips republished (the recorded exception in CLAUDE.md). The proposal's Westminster-only API-polygon approach was dropped |
| `mhclg_imd_2025` | [English Indices of Deprivation 2025](https://www.gov.uk/government/statistics/english-indices-of-deprivation-2025) — LSOA + LA files | ~5-yearly (Oct 2025) | XLSX/CSV/GPKG | A | Effectively static until ~2030 — weekly runs will simply report it unchanged, which is free (HF skips no-op commits) |
| `ons_population_estimates` | [Mid-year population estimates by LA](https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationestimates) | Annual | XLSX | B | ONS landing-page scrape |
| `gla_population_projections` | [GLA population projections](https://data.london.gov.uk/dataset/) (borough, housing-led) | Annual | CSV/XLSX | A | Datastore direct download |
| `wcc_spending_over_500` | [Westminster payments to suppliers > £500](https://www.westminster.gov.uk/about-council/transparency/spending-procurement-and-data-transparency) | Quarterly | CSV | B | Also listed on [data.gov.uk](https://www.data.gov.uk/dataset/7922f997-6517-4fdd-bd8b-cd3c49f98176/local-authority-spending-over-500-westminster). Our own council's transparency data — accumulating quarterly files needs a small design tweak (fetch newest file(s), stable per-quarter names) |
| `dft_naptan_london_stops` | [NaPTAN public transport access nodes](https://beta-naptan.dft.gov.uk/download) — every bus stop/station | Daily | CSV/XML | A | All London boroughs share one ATCO area (490), so "Westminster only" needs a spatial/locality filter in the tidy step |
| `tfl_cycling_data` | [TfL cycling open data](https://cycling.data.tfl.gov.uk/) — Santander hire journeys, active-travel counts | Varies | CSV | A | Open S3 bucket. Journey files are chunky; the quarterly Active Travel Counts programme + daily hire totals are the tractable subsets |
| — | [Bus Open Data Service (BODS)](https://www.bus-data.dft.gov.uk/) timetables/AVL | Live | API | C | Needs account; live AVL doesn't suit weekly snapshots. Only worth it with a concrete use case |
| — | [GLA High Streets Data Service](https://data.london.gov.uk/high-streets-data-service/) footfall/spend | Monthly | Licensed | D | Aggregated card/footfall data is licence-restricted, not open — check borough access separately |

---

# Part 2 — Lesser-known / higher-novelty sources

Researched 2026-07-09. The Part 1 sources are well-trodden: anyone can pull a
gov.uk ODS. The sources below are where this repo earns its keep, for two
reasons:

- **Live registers with no history.** Several of these (CQC directory, OCOD,
  Historic England, GBFS feeds, ModernGov, PlanIt) are *current-state*
  registers — the publisher overwrites them and keeps no public archive.
  A weekly snapshot, with history kept by the HF repo's commits and the
  monthly `archive-*` tags, creates a change-over-time dataset that
  **exists nowhere else**.
- **Nobody collates them per-borough.** Filtering national/London feeds to
  Westminster and publishing tidy parquets side-by-side with the official
  series is the collation value the user of this repo actually wants.

Licence discipline matters more here: several are CC BY or ODbL (attribution
/ share-alike) rather than OGL — record it properly in `datapackage.json`.

## 7. Housing & property (novel angles)

| Proposed source | Dataset | Cadence | Format | Feas. | Notes |
| --- | --- | --- | --- | --- | --- |
| `iab_airbnb_london` | [Inside Airbnb — London](https://insideairbnb.com/get-the-data/) — every Airbnb listing: location, type, nights, availability, host concentration | Quarterly | CSV (.gz) | A | Short-term lets are one of Westminster's biggest housing-enforcement issues and there is **no official dataset at all**. Filter to Westminster; archive builds the longitudinal record Airbnb won't publish. CC BY 4.0 |
| `hmlr_ocod_westminster` | [Overseas companies owning property (OCOD)](https://use-land-property-data.service.gov.uk/datasets/ocod) | Monthly (2nd working day) | CSV | C | Free account + licence acceptance → API/download. Westminster is the epicentre of offshore-owned property in England. Companion `hmlr_ccod` (UK corporate owners) same portal |
| `che_companies_westminster` | [Companies House free company data](https://download.companieshouse.gov.uk/en_output.html) — all live companies, registered-office postcode | Monthly | CSV (zip) | A | ~450 MB national zip → filter to Westminster postcodes (W1/SW1/W2/NW1/W9…) into the tidy parquet. Tracks business churn street-by-street *(URL pattern to confirm at build time)* |
| `gla_chain_rough_sleeping` | [CHAIN rough sleeping reports](https://data.london.gov.uk/dataset/rough-sleeping-in-london-chain-reports-2n88x) — quarterly street-count detail | Quarterly | Mostly PDF ⚠ | B/D | Far richer than the annual MHCLG snapshot (Westminster ~990 people vs Camden ~300). Borough reports are PDFs — check newer releases for data tables before building; even archiving the Westminster PDF has value |

## 8. Parking, kerbside & mobility (novel angles)

| Proposed source | Dataset | Cadence | Format | Feas. | Notes |
| --- | --- | --- | --- | --- | --- |
| `lt_pcn_appeals` | [London Tribunals appeal statistics](https://www.londontribunals.gov.uk/about/annual-reports-and-appeal-statistics) + [statutory register of appeals](https://www.londontribunals.gov.uk/about/registers-appeals) | Annual (reports); live (register) | PDF / web register | B/D | The *independent* view of Westminster's PCN quality — appeal rates and win rates against the council. Annual report tables are extractable; the live register is scrape-heavy but updated real-time and never archived |
| `gbfs_dockless_westminster` | Lime / Forest / Voi dockless e-bike GBFS feeds — live vehicle positions & fleet size | Live (snapshot weekly) | JSON | B | Dockless clutter is a live Westminster policy fight with no public evidence base. Lime pattern: `data.lime.bike/api/partners/v2/gbfs/london/…` *(endpoints unverified — confirm per operator; some London feeds are TfL-gated)*. Weekly snapshot → fleet counts in Westminster over time |
| `tfl_numbat_station_demand` | [TfL NUMBAT / network demand](https://crowding.data.tfl.gov.uk/) — station entry/exit + link loads, 15-min granularity | Annual | XLSX (bucket) | A | Open bucket, barely known outside transport-modelling circles. Filter to the ~30 Westminster stations. Foot-traffic proxy for the West End |
| `osm_westminster_street_assets` | OpenStreetMap Overpass extracts for Westminster — cycle parking, benches, water fountains, EV chargers, car-club bays, parklets | Continuous (snapshot weekly) | JSON→parquet | A | Overpass API, no key. The only citywide inventory of most street furniture. ODbL licence (share-alike) — keep extracts clearly attributed |

## 9. Registers worth snapshotting (licensing, heritage, care, civic)

| Proposed source | Dataset | Cadence | Format | Feas. | Notes |
| --- | --- | --- | --- | --- | --- |
| `cqc_care_directory_westminster` | [CQC care directory](https://www.cqc.org.uk/about-us/transparency/using-cqc-data) — every regulated care location + ratings | Weekly | CSV | A/B | Predictable URL pattern (`/sites/default/files/YYYY-MM/DD_Month_YYYY_CQC_directory.csv`) — scrape the page for the current link. Filter to Westminster. Care-market change over time |
| `he_nhle_westminster` | [Historic England NHLE](https://opendata-historicengland.hub.arcgis.com/) — listed buildings, scheduled monuments, heritage at risk | Daily updates | CSV/GeoJSON | A | ~11,000 listed buildings in Westminster; listings/delistings/at-risk changes are genuinely newsworthy and nobody tracks them. ArcGIS Hub has direct export URLs |
| `gla_cultural_infrastructure` | [GLA Cultural Infrastructure Map](https://data.london.gov.uk/dataset/cultural-infrastructure-map-2ko88) — pubs, grassroots music venues, LGBTQ+ venues, theatres | Occasional (2019, 2023 audit) | CSV | A | Venue loss is a licensing/public-realm battleground; snapshot whenever GLA re-audits |
| `wcc_moderngov_decisions` | Westminster ModernGov ([westminster.moderngov.co.uk](https://westminster.moderngov.co.uk/ieDocHome.aspx?bcr=1)) — committee meetings, agendas, decisions, forward plan | Continuous | XML (mgWebService) | B | Nearly every council runs ModernGov; its `mgWebService.asmx` XML API is public but undocumented ([known pattern](https://github.com/DemocracyClub/LGSF)). Weekly pull of decisions + forward plan = machine-readable democratic record *(confirm the API is enabled on Westminster's instance)* |
| `tm_toilets_london` | [Great British Public Toilet Map](https://www.toiletmap.org.uk/dataset) — 14,000+ publicly accessible toilets | Continuous | API/export | B | Classic public-realm gap dataset (no official register exists). CC BY 4.0; check export mechanics on the dataset page |
| `ms_fixmystreet_westminster` | [FixMyStreet reports](https://www.fixmystreet.com/reports) — public street-fault reports | Continuous | Dashboard/CSV | C/D | Council-officer dashboard access is free with a .gov.uk email — worth requesting. Public site is scrapeable but be polite. mySociety themselves caveat cross-area comparisons |

## 10. Infrastructure & utilities

| Proposed source | Dataset | Cadence | Format | Feas. | Notes |
| --- | --- | --- | --- | --- | --- |
| `ukpn_network_capacity` | [UK Power Networks open data portal](https://ukpowernetworks.opendatasoft.com/) — substation headroom, LCT connections (EV chargers, heat pumps, solar) by area | Varies | CSV/API (Opendatasoft) | A | 55+ datasets, standard Opendatasoft export API. Grid headroom is *the* binding constraint on Westminster EV-charging and heat-decarbonisation plans, and almost nobody outside DNOs looks at it |
| `ofcom_connected_nations` | [Ofcom Connected Nations](https://www.ofcom.org.uk/phones-and-broadband/coverage-and-speeds/connected-nations-20252/data-downloads-2025) — broadband speeds & 4G/5G coverage by LA | Annual + spring update | CSV (zip) | B | LA-level files; the download page moves each year → scrape the collection page |
| `ea_flood_monitoring` | [EA flood-monitoring API](https://environment.data.gov.uk/flood-monitoring/doc/reference) — Thames levels, flood warnings/alerts | Live (15-min) | JSON | A | No key, well-documented REST API. Weekly snapshot of warnings issued + station levels for the Westminster river frontage *(basement flooding is a recurring WCC issue)* |
| `planit_westminster_apps` | [PlanIt](https://www.planit.org.uk/) — community-aggregated planning applications, all UK authorities, JSON API | Daily | JSON API | A | Free API over 20.5m applications. Overlaps Planning London Datahub but is simpler to query (`/api/applics/json?auth=westminster&…`) and covers authorities PLD misses. Good fallback if PLD's Elasticsearch proves brittle |

---

## Cross-cutting build notes

- **Big-file sources** (STATS19, AADF, price paid, NaPTAN, cycling journeys):
  publish the raw file *only when reasonably sized*; otherwise skip the raw
  and publish a London/Westminster-filtered long-format parquet as the
  primary artefact, citing the stable upstream raw in `datapackage.json` —
  the pattern `police_crime_london` established (CLAUDE.md's recorded
  exception to publish-the-raw-file: only valid when the publisher retains
  the raw at stable URLs, and the parquets then become mandatory in
  validate() rather than best-effort). Filtering also keeps files small
  enough for browser dashboards.
- **gov.uk statistical data sets** (all the `B` rows on gov.uk): one shared
  helper could scrape any gov.uk collection page for an attachment matching a
  regex — worth extracting into `lib/` once we build the second such source
  (RDC would be it).
- **API-key sources** (EPC, Street Manager, BODS): needs a `secrets:` entry in
  the workflow + graceful skip when the secret is absent locally. Do one of
  these after several A/B sources are bedded in.
- **Westminster-internal gaps** (bulk licensing register, current parking bay
  inventory, CPZ boundaries as GIS): the open web doesn't have them — the
  council does. Worth an internal ask before resorting to scraping/FOI.
- **Snapshot-of-a-live-register sources** (Part 2's CQC, OCOD, NHLE, GBFS,
  ModernGov, toilet map): the published file should be the *current state*
  under a stable filename; the change history accumulates for free in the
  HF repo's commit history, the monthly `archive-YYYY-MM` tags and
  catalog.json's git history — the diffs ARE the product.
- **Non-OGL licences** (Inside Airbnb CC BY, OSM ODbL, toilet map CC BY):
  fine to republish with attribution, but ODbL is share-alike — derived
  files from OSM must be flagged as ODbL in `datapackage.json`, not OGL.
- **Politeness for scraped/community sources** (FixMyStreet, London
  Tribunals register, ModernGov): weekly, one request per run, accurate
  User-Agent (already in `lib/common.py`) — these are small operations, do
  not hammer them.
