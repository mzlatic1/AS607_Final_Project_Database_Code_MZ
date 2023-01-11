library(DBI)
library(RPostgres)
library(tidyverse)

# Connect to Postgres database #
con <- dbConnect(RPostgres::Postgres(),
                 dbname = 'gis-spatialdbs',
                 port = 5432,
                 host = 'jhu430607sdb.cnzmwrn1t8z5.us-east-2.rds.amazonaws.com',
                 user = 'student14',
                 password = rstudioapi::askForPassword('Input Password:'))

# Query for respected data table and export it as a R dataframe #
query_prod_fc <- dbGetQuery(con, 'select * from "gis-spatialdbs".student14.as607_final_project_active_and_historical_fires_bc_can;')
prod_df <- as.data.frame(query_prod_fc)

todays_date <- Sys.Date()

folder_output <- '~/Documents/as607_final_project_R_results'
setwd(folder_output)
sink(paste0('Polynomial_Regression_Results_', todays_date, '.txt')) # Write statistical results via txt file

### Calculating Number of Fires Per Year ###
# Aggregates R dataframe to count the number of records per year #
year_freq <- aggregate(prod_df$fire_year, list(num=prod_df$fire_year), length)
colnames(year_freq)[2] <- 'Num_of_Fires'
colnames(year_freq)[1] <- 'Year'

year <- year_freq$Year
num_fires <- year_freq$Num_of_Fires

# Executes polynomial regression and writes summary statistics via txt file
yr_regr <- lm(num_fires ~ year + I(year^2) + I(year^3) + I(year^4) + I(year^5) + I(year^6))
summary(yr_regr)

# Graphs points and regression, then saves it to output folder directory
plot_yr <- ggplot(year_freq, aes(year, num_fires)) + geom_point() + stat_smooth(method = 'lm', formula = y ~ poly(x, 6)) +
  ggtitle('Total Number of Forest Fires, per Year, Over Time') + theme(plot.title = element_text(hjust = 0.5)) +
  xlab('Year') + ylab('Number of Forest Fires') + coord_cartesian(ylim = c(0, NA))
ggsave(plot_yr, file = paste0('Plot_Year_', todays_date, '.png'))

### Calculating Size of Fires Per Year ###
# Aggregates R dataframe by adding up the total size of fires per year #
year_size <- aggregate(prod_df$current_size, list(num=prod_df$fire_year), sum)
colnames(year_size)[2] <- 'Size_of_Fires'
colnames(year_size)[1] <- 'Year'

size_year <- year_size$Year
size_fires <- year_size$Size_of_Fires

# Executes polynomial regression and writes summary statistics via txt file
size_regr <- lm(size_fires ~ size_year + I(size_year^2) + I(size_year^3) + I(size_year^4) + I(size_year^5) + I(size_year^6))
summary(size_regr)

# Graphs points and regression, then saves it to output folder directory
plot_size <- ggplot(year_size, aes(size_year, size_fires)) + geom_point() + stat_smooth(method = 'lm', formula = y ~ poly(x, 6)) +
  ggtitle('Total Size of Forest Fires, per Year, Over Time') + theme(plot.title = element_text(hjust = 0.5)) +
  xlab('Year') + ylab('Size of Forest Fires (HA)')
ggsave(plot_size, file = paste0('Plot_Size_', todays_date, '.png'))

sink() # Closes txt file and redirects outputs back to console
