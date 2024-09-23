source(here::here("R/00-packages.R"))

read_ABPEs_online <- function(dataset_name = "abpe15072024", table_name=1, sleep=1){
  
  if(1==1){
    warning("broken currently - please use downloaded function")
    return()
  }
  
  url <- paste0("https://www.ons.gov.uk/peoplepopulationandcommunity", 
                "/populationandmigration/populationestimates/datasets", 
                "/adminbasedpopulationestimatesforlocalauthoritiesinenglandandwales")
  Sys.sleep(sleep)
  
  if (httr::http_error(url)) {
    stop("ONS projections url not found")
  }
  response <- httr::GET(url)
  webpage <- read_html(content(response, "text"))
  xlsx_links <- html_attr(html_nodes(webpage, "a[href$='.xlsx']"), "href")
  
  xlsx_url <- paste0(
    url,
    xlsx_links[str_detect(xlsx_links, dataset_name)])
  
  if(length(xlsx_url)!=1){stop("More than one or no xlsx file found")}
  
  invisible(httr::GET(xlsx_url, httr::write_disk(temp_file <- tempfile(fileext = ".xlsx"))))
  ons_abpes <- readxl::read_excel(temp_file,
                                  sheet = table_name, 
                                  skip = 6)
  return(ons_abpes)
}

read_ABPEs_downloaded <- function(file_path = "./data/abpe15072024.xlsx",
                                  table_name = "1",
                                  lad_filter="BNSSG"){
  
  if(lad_filter=="BNSSG"){
    lad_filter <- c("Bristol, City of", "North Somerset", "South Gloucestershire")
  }
  
  table_title <- readxl::read_excel(file_path,
                                    sheet = table_name, 
                                    range="A1:A1") |> 
    names() 
  
  # find the number of rows to skip - it's where you first see 'LAD code' in col 1
  num_to_skip <- readxl::read_excel(file_path,
                                   sheet = table_name, 
                                   range="A1:A100") |> 
    mutate(a=ifelse(str_detect(!!sym(table_title),"LAD code"),1,0)) |> 
    mutate(r=row_number()) |> 
    filter(a==1) |> 
    pull(r)
  
  # read in the table
  ons_abpes <- readxl::read_excel(file_path,
                                  sheet = table_name, 
                                  skip = num_to_skip) |> 
    janitor::clean_names() |> 
    mutate(table_title = table_title)
  
  if(!sum(str_detect(lad_filter,"ALL"))){
    ons_abpes <- ons_abpes |> filter(lad_name %in% lad_filter)
  }
  
  return(ons_abpes)
}

ons_pop_events_raw_tbl <- map(
  as.character(1:5),function(x){read_ABPEs_downloaded(table_name=x)}) |> 
  bind_rows()

# tidy up 
ons_pop_events_clean_tbl <- ons_pop_events_raw_tbl |> 
  # make simple
  mutate(
    event = case_when(
      str_detect(tolower(table_title),"population") ~ "population",
      str_detect(tolower(table_title),"births") ~ "births",
      str_detect(tolower(table_title),"deaths") ~ "deaths",
      str_detect(tolower(table_title),"immigration") ~ "immigrations",
      str_detect(tolower(table_title),"emigration") ~ "emigrations",
      TRUE ~ table_title)) |> 
  # combine all the different metrics
  mutate(num_people = ifelse(
    !is.na(live_births),live_births,
    ifelse(
      !is.na(deaths), deaths,
      ifelse(
        !is.na(population_average_mean_note_3_note_4),
        population_average_mean_note_3_note_4,
        ifelse(
          !is.na(combined_emigration_average_mean_note_3_note_4_note9),
          combined_emigration_average_mean_note_3_note_4_note9,
          ifelse(
            !is.na(combined_immigration_average_mean_note_3_note_4_note_9),
            combined_immigration_average_mean_note_3_note_4_note_9,
            NA)))))) |> 
  # sort the years out
  mutate(
    year = ifelse(
      !is.na(mid_year),mid_year,
      ifelse(
        !is.na(year_ending_30_june),year_ending_30_june,
        as.numeric(NA)))) |> 
  select(year, event, age=age_note_5,sex,num_people,area=lad_name) |>
  # get rid of sex
  summarise(num_people = sum(num_people), .by= c(year,event,age, area))

arrow::write_parquet(ons_pop_events_raw_tbl, 
                     here::here("data/ons_pop_events_raw_tbl.parquet"))
arrow::write_parquet(ons_pop_events_clean_tbl, 
                     here::here("data/ons_pop_events_clean_tbl.parquet"))
