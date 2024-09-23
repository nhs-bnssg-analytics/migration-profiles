source(here::here("R/00-packages.R"))
ons_matrix <- readxl::read_excel(here::here("data/detailedestimates2022on2021and2023las.xlsx"),
                                 sheet="2022 on 2023 LAs") |> 
  janitor::clean_names()

glimpse(ons_matrix)

ons_pop_events_raw_tbl <- arrow::read_parquet(here::here("data/ons_pop_events_raw_tbl.parquet"))

la_lookup <- tibble::tribble(
  ~lad_name, ~lad_code,
  "Bristol, City of", "E06000023",
  "North Somerset", "E06000024",
  "South Gloucestershire", "E06000025"
)

bnssg_internal_mig_tbl <- ons_matrix |> 
  filter(outla %in% la_lookup$lad_code | 
           inla %in% la_lookup$lad_code) |> 
  mutate(
    outla_group = case_when(
      outla %in% la_lookup$lad_code ~ outla,
      TRUE ~ "Other Internal"
    ),
    inla_group = case_when(
      inla %in% la_lookup$lad_code ~ inla,
      TRUE ~ "Other Internal"
    )
  ) |> 
  summarise(across(starts_with("age_"), sum), .by=c(outla_group, inla_group)) |> 
  pivot_longer(cols=starts_with("age_"), names_to="age_group", values_to="count") 


# Proportional movement
bnssg_internal_mig_tbl |> 
  summarise(count=sum(count), .by=c(outla_group, inla_group)) |> 
  group_by(outla_group) |> 
  mutate(prop_out = count/sum(count)) |> 
  left_join(la_lookup |> rename(outla_name = lad_name), by=c("outla_group"="lad_code")) |>
  mutate(outla_name = ifelse(is.na(outla_name),"z Other England&Wales LA",outla_name)) |> 
  left_join(la_lookup |> rename(inla_name = lad_name), by=c("inla_group"="lad_code")) |>
  mutate(inla_name = ifelse(is.na(inla_name),"z Other England&Wales LA",inla_name)) |> 
  ggplot(aes(x=outla_name, y=prop_out, fill=inla_name)) + 
  geom_col() + 
  # add in %s to the stacked bars
  geom_text(aes(label=scales::percent(prop_out, accuracy=0.1)), position=position_stack(vjust=0.5)) +
  labs(
    x="LA that the people migrated FROM",
    y="Proportion of Internal Migration",
    fill="LA that the people migrated TO",
    title = "17.6% of people who moved out of Bristol but stayed in England&Wales in the year Jun2022 - Jun2023 moved to South Glos",
    caption = paste0("source: ONS detailed 2022 on 2023 LAs \n",
                     "https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/internalmigrationinenglandandwales")) +
  scale_fill_manual(values = viridis::magma(5)[2:5])

# Raw numbers movement
bnssg_internal_mig_tbl |> 
  summarise(count=sum(count), .by=c(outla_group, inla_group)) |> 
  group_by(outla_group) |> 
  left_join(la_lookup |> rename(outla_name = lad_name), by=c("outla_group"="lad_code")) |>
  mutate(outla_name = ifelse(is.na(outla_name),"z Other England&Wales LA",outla_name)) |> 
  left_join(la_lookup |> rename(inla_name = lad_name), by=c("inla_group"="lad_code")) |>
  mutate(inla_name = ifelse(is.na(inla_name),"z Other England&Wales LA",inla_name)) |> 
  ggplot(aes(x=outla_name, y=count, fill=inla_name)) + 
  geom_col() + 
  # add in %s to the stacked bars
  geom_text(aes(label=scales::comma(round(count,-2))), position=position_stack(vjust=0.5)) +
  labs(
    x="LA that the people migrated FROM",
    y="Number of Internal Migrants",
    fill="LA that the people migrated TO",
    title = "3,400 people moved out of Bristol to North Somerset in the year Jun2022 - Jun2023 moved to South Glos",
    caption = paste0("source: ONS detailed 2022 on 2023 LAs \n",
                     "https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/internalmigrationinenglandandwales")
  )
