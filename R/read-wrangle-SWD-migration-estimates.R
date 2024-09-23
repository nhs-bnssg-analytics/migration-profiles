source(here::here("R/00-packages.R"))


# get mid-2023 estimates for annual changes since mid-2022
swd_bmd_nums_tbl <- 
  map(c("Bristol, City of", "North Somerset", "South Gloucestershire"),
      function(x){
        DPMmicrosim::get_births_migrations_deaths_proportions(
  start_month = "2023-06",
  source_or_preload = "source",
  method = 1,
  sql_con = DPMmicrosim::get_sql_con(),
  min_age = 0,
  compare_against = 12,
  combine_immigration_emigration = F,
  output_proportions_or_numbers = "numbers",
  age_groups = T,
  exact_ages = T,
  chosen_area = x) |> mutate(area = x)}) |> 
  bind_rows()

swd_bmd_nums_tbl <- swd_bmd_nums_tbl |>
  select(-age_group, -age_cs_state) |> 
  dpm:::add_age_group_column()

# get the population estimates (GP adjusted)
initial_pop <- map(
  c("Bristol, City of", "North Somerset", "South Gloucestershire"),
    function(x){
      DPMmicrosim::create_individuals_in_initial_population(
  start_month="2023-06",
  method=1,
  sql_con=DPMmicrosim::get_sql_con(),
  min_age=0,
  chosen_area = x) |> mutate(area=x)}) |> 
  bind_rows()

initial_pop <- initial_pop |> 
  summarise(num_people = n(), .by=c(state_name, age, area)) |> 
  mutate(event="population") |> 
  select(event, state_name, age, num_people, area) |> 
  complete(event, state_name, age, area, fill=list(num_people=0)) |> 
  arrange(area, age, state_name)

# combine
swd_pop_events_tbl <- bind_rows(
  initial_pop, 
  swd_bmd_nums_tbl |> select(-age_group)
) |> 
  mutate(year = 2023)

# Look at estimated in-outs
swd_bmd_nums_tbl |>
  ggplot(aes(x=age,y=num_people)) + 
  geom_col()  + 
  facet_grid(area~event) + 
  labs(title = "SWD migration events in the year ending Jun 2023")

write_parquet(swd_pop_events_tbl,
              here::here("data/swd_pop_events_tbl.parquet"))
