source(here::here("R/00-packages.R"))

# We can't use get_births_migrations_deaths_proportions because of known practices
# that drop in and out

# we're aiming for this 
swd_pop_events_tbl <- arrow::read_parquet(here::here("data/swd_pop_events_tbl.parquet"))
glimpse(swd_pop_events_tbl)

# this is our lookup code for whether to include the person
missing_data <- readxl::read_excel(
  "./data/MissingDataReport - taken20240923.xlsm",
  sheet="Data"
) |> 
  janitor::clean_names() |> 
  filter(dataset=="Attributes")

missing_data |> count(status_desc)

glimpse(missing_data)

# Initial Params
month_of_interest <- "2023-06-01"
my_chosen_area <- "North Somerset"

# function to calculate it. Note it uses missing_data which is hardcoded and presumed 
# in the environment
calc_migrants_without_missing <- function(
    month_of_interest, chosen_area, return_nhs_num_level=F){

  comparison_month <- format(lubridate::as_date(month_of_interest) - months(12),"%Y-%m-%d")
  
  # takes about 10 seconds to get the migrant data
  nhs_num_migrant_tbl <- DPMmicrosim:::get_bmd_vals_matching_method(
    sql_con = DPMmicrosim::get_sql_con(), 
    start_month_date_char = month_of_interest, 
    compare_against_month_date_char = comparison_month, 
    combine_immigration_emigration = F, 
    min_age = 17, 
    age_groups = T, 
    exact_ages = T, 
    chosen_area = chosen_area, 
    return_raw_records = T) |> 
    filter(status %in% c("immigrant","emigrant"))
  
  la_finder_tbl <- DPMmicrosim::get_sql_con() |> 
    DPMmicrosim:::get_sql_table_source_new_cambridge_score() |> 
    filter(attribute_period %in% c(comparison_month, month_of_interest)) |> 
    select(attribute_period, nhs_number, practice_code) |> 
    collect()
  
  nhs_num_migrant_tbl <- nhs_num_migrant_tbl |> 
    #### EMIGRANTS 
    #first get the places people supposedly left FROM. We want to know - did they
    # actually not exist in the data because their practice wasn't reporting during
    # month_of_interest
    left_join(
      la_finder_tbl |> 
        filter(attribute_period==comparison_month) |> 
        select(nhs_number, emigrant_practice_code = practice_code),
      by=c("nhs_number")) |> 
    # mow find out whether there was missing data from the practice in the month 
    # they supposedly no longer existed in
    left_join(
      missing_data |> 
        filter(dataset=="Attributes") |> 
        mutate(attribute_period = format(period,"%Y-%m-%d")) |> 
        filter(attribute_period %in% c(month_of_interest)) |> 
        select(practice_code, 
               emigrant_status_desc = status_desc,
               emigrant_status_code = status_code),
      by=c("emigrant_practice_code"="practice_code")) |> 
    #### IMMIGRANTS
    # now do the same for the places people supposedly arrived TO. We want to know - 
    # did they actually not exist in the data in the past because their practice 
    # wasn't reporting during comparison_month
    left_join(
      la_finder_tbl |> 
        filter(attribute_period==month_of_interest) |> 
        select(nhs_number, immigrant_practice_code = practice_code),
      by=c("nhs_number")) |> 
    left_join(
      missing_data |> 
        filter(dataset=="Attributes") |> 
        mutate(attribute_period = format(period,"%Y-%m-%d")) |> 
        filter(attribute_period %in% c(comparison_month)) |> 
        select(practice_code, 
               immigrant_status_desc = status_desc,
               immigrant_status_code = status_code),
      by=c("immigrant_practice_code"="practice_code"))
  
  
  # final step - identify those who have actually been caught up in the missingness
  nhs_num_migrant_tbl <- nhs_num_migrant_tbl |> 
    mutate(migrant_data_status = case_when(
      status=="immigrant" & immigrant_status_desc %in% c("Closed","Missing") ~ "missing",
      status=="emigrant" & emigrant_status_desc %in% c("Closed","Missing") ~ "missing",
      TRUE ~ "complete"
    ))
  
  if(return_nhs_num_level){return(nhs_num_migrant_tbl)}
  
  # aggregate up at the end
  summary_tbl <- nhs_num_migrant_tbl |> 
    filter(migrant_data_status!="missing") |>
    summarise(num_people=n(),.by=c(state_name,status,age)) |> 
    mutate(year = lubridate::year(lubridate::as_date(month_of_interest)),
           area = chosen_area,
           event = case_when(
             status=="emigrant" ~ "emigrations",
             status=="immigrant" ~ "immigrations",
             TRUE ~ status
           )) |> 
    select(event,
           state_name,
           age,
           area,
           num_people,
           year) |> 
    mutate(method = "removing known missing GP practices")
  
  return(summary_tbl)
}

swd_migrations_tbl <- 
  map(c("Bristol, City of", "North Somerset", "South Gloucestershire"),
      function(x){
        calc_migrants_without_missing(
          month_of_interest,chosen_area = x, return_nhs_num_level = F)}) |> 
  bind_rows()

write_parquet(swd_migrations_tbl,
              here::here("data/swd_migrations_tbl.parquet"))
