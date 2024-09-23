

plot_ons_pop_pyramid<- function(abpes_tbl){
  ons_tbl |> 
    filter(mid_year==max(mid_year)) |> 
    mutate(pop = ifelse(sex=="Male",
                        population_average_mean_note_3_note_4,
                        -population_average_mean_note_3_note_4)) |> 
    ggplot(aes(x=age_note_5, 
               y=pop,
               fill=sex)) + 
    geom_col() + 
    facet_wrap(~lad_name,scale="free_x") + 
    coord_flip()
}