library(ambiorix)
library(RSQLite)
library(jsonlite)
library(data.table)



flights_dt <- as.data.table(nycflights13::flights)
flights_dt[, delayed := dep_delay > 15]

avg_delay <- flights_dt[, .(avg_dep_delay = mean(dep_delay, na.rm = TRUE)), by = carrier]
top_destinations <- flights_dt[, .N, by = dest][order(-N)]


#creating a SQLite db to dump the processed data
conn <- dbConnect(SQLite(), "flights.db")
dbExecute(conn, "DROP TABLE IF EXISTS flights")
dbExecute(conn, "
  CREATE TABLE flights (
    id INTEGER PRIMARY KEY,
    year INTEGER,
    month INTEGER,
    day INTEGER,
    dep_time INTEGER,
    sched_dep_time INTEGER,
    dep_delay INTEGER,
    arr_time INTEGER,
    sched_arr_time INTEGER,
    arr_delay INTEGER,
    carrier TEXT,
    flight INTEGER,
    tailnum TEXT,
    origin TEXT,
    dest TEXT,
    air_time INTEGER,
    distance INTEGER,
    hour INTEGER,
    minute INTEGER,
    time_hour REAL,
    delayed BOOLEAN
  )
")
dbExecute(conn, "DROP TABLE IF EXISTS avg_delay")
dbExecute(conn, "
  CREATE TABLE avg_delay (
    id INTEGER PRIMARY KEY,
    carrier TEXT,
    avg_dep_delay REAL
  );
")

dbExecute(conn, "DROP TABLE IF EXISTS top_destinations")
dbExecute(conn, "
  CREATE TABLE top_destinations (
  id INTEGER PRIMARY KEY,
   dest TEXT,
   N INTEGER
  );
")

dbWriteTable(conn, "flights", flights_dt, append=TRUE)
dbWriteTable(conn, "avg_delay", avg_delay, append = TRUE)
dbWriteTable(conn, "top_destinations", top_destinations, append = TRUE)
dbDisconnect(conn)


app <- Ambiorix$new()
conn <- dbConnect(SQLite(), "flights.db")
 

# GET /flight/:id - Get flight details by ID
app$get("/flight/:id", function(req, res) {
  flight <- dbGetQuery(conn, sprintf("SELECT * FROM flights WHERE id = %s", req$params$id))
  if (nrow(flight) != 0) {
    return(res$set_status(201L)$json(flight))
  }else{
    
    # otherwise it is a bad request change response status to 400
    msg <-  sprintf("There's no Flight with id  %s", req$params$id)
    return(res$set_status(400L)$json(msg))
  } 
})



#POST Endpoint Logic
# Credit: https://ambiorix.dev/blog/parse-raw-json/

box::use(
  webutils[parse_http],
)
parse_req <- \(req) {
  parse_http(
    body = req$rook.input$read(),
    content_type = req$CONTENT_TYPE
  )
}


app$post('/flight', function(req, res){
  content <- parse_req(req)
  content<-as.data.table(content)
  dbWriteTable(conn, "flights", content, append = TRUE)
 return( res$json(content))
})


##GET /check-delay/:id

app$get("/check-delay/:id", function(req, res) {
  id<-req$params$id
  flight <- dbGetQuery(conn, sprintf("SELECT * FROM flights WHERE id = %s", id))
  if (nrow(flight) != 0) {
    delayed_status <- flight$delayed
    return(res$json(list(delayed = delayed_status)))
  }else{
    
    # otherwise it is a bad request change response status to 400
    msg <-  sprintf("There's no Flight with id  %s", req$params$id)
    return(res$set_status(400L)$json(msg))
  } 
})




# GET /avg-dep-delay?id=given-airline-name

app$get("/avg-dep-delay", function(req, res) {
  airline <- req$query$id
  
  if (is.null(airline)) {
    # If no airline is provided, return all the airlines
    carrier <- dbGetQuery(conn, "SELECT carrier, avg_dep_delay FROM avg_delay")
  } else {
    carrier <- dbGetQuery(conn, sprintf("SELECT avg_dep_delay FROM avg_delay WHERE carrier = '%s'", airline))
    
    if (nrow(carrier) == 0) {
      msg <- sprintf("There's no flight data for airline %s", airline)
      return(res$set_status(400L)$json(list(error = msg)))
    }
  }
  
  return(res$json(carrier))
})




##GET /top-destinations/:n

app$get("/top-destinations/:n", function(req, res) {
  destinations<-req$params$n
  destinations<-as.integer(destinations)
  all_destinations<-dbGetQuery(conn, "SELECT * FROM top_destinations")
  log<-destinations <= nrow(all_destinations)
  if(log){
  destinations <- dbGetQuery(conn, sprintf("SELECT * FROM top_destinations LIMIT %s", destinations))
  return(res$set_status(201L)$json(destinations))
  }
  
  msg <- sprintf("Not enough top destinations in db for %s destinations", destinations)
  return(res$set_status(404L)$json(list(error = msg)))
  
})



# PUT /flights/:id
app$put("/flights/:id", function(req, res) {
  id <- as.integer(req$params$id)
  new_details <- parse_req(req)
  new_details <- as.data.table(new_details)
  
  #get the flight to update
  existing_flight <- dbGetQuery(conn, sprintf("SELECT * FROM flights WHERE id = %d", id))
  if (nrow(existing_flight) == 0) {
    msg <- sprintf("No flight found with id %d", id)
    return(res$set_status(404L)$json(list(error = msg)))
  }
  
  # Update the flight details
  setnames(new_details, names(new_details), paste0("\"", names(new_details), "\""))
  set_query <- paste(sprintf("%s = '%s'", names(new_details), new_details), collapse = ", ")
  update_query <- sprintf("UPDATE flights SET %s WHERE id = %d", set_query, id)
  
  dbExecute(conn, update_query)
  
  # Return updated flight
  updated_flight <- dbGetQuery(conn, sprintf("SELECT * FROM flights WHERE id = %d", id))
  response <- list(
    message = sprintf("Flight with ID %d has been updated successfully.", id),
    new_details = updated_flight
  )
  
  return(res$json(response))
  
})



## DELETE /:id

app$delete("/:id", function(req, res) {
  id <- as.integer(req$params$id)
  
  # Check if the flight exists
  existing_flight <- dbGetQuery(conn, sprintf("SELECT * FROM flights WHERE id = %d", id))
  
  if (nrow(existing_flight) == 0) {
    msg <- sprintf("No flight found with id %d", id)
    return(res$set_status(404L)$json(list(error = msg)))
  }
  
  # Delete the flight
  dbExecute(conn, sprintf("DELETE FROM flights WHERE id = %d", id))
  
  msg <- sprintf("Flight with id %d successfully deleted", id)
  return(res$json(list(message = msg)))
})


app$start(3000)
on.exit(dbDisconnect(conn))
