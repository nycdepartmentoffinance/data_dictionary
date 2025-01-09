# data_dictionary


```{r, include=FALSE}
library(dplyr)
library(readr)
library(DT)

table_data <- readr::read_csv("data_dictionary_table_data.csv")
```


```{r}
#| echo: false
#| include: false

DT::datatable(table_data, escape = FALSE, filter = 'top', rownames = FALSE)

```


