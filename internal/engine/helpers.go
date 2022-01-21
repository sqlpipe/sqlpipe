package engine

// "sqlpipe/app/models"

// func GetConnections() (connections []models.Connection) {
// 	db := helpers.GetDb()
// 	db.Find(&connections)
// 	return connections
// }

// func CheckConnectionAsync(connection models.Connection, wg *sync.WaitGroup) (canConnect bool) {
// 	defer wg.Done()

// 	dsConn := GetDs(connection)
// 	_, driverName, connString := dsConn.getConnectionInfo()
// 	db, err := sql.Open(driverName, connString)

// 	if err != nil {
// 		return false
// 	}

// 	if err := db.Ping(); err != nil {
// 		return false
// 	}

// 	return true
// }

// func CheckConnection(connection models.Connection) (canConnect bool) {
// 	dsConn := GetDs(connection)
// 	_, driverName, connString := dsConn.getConnectionInfo()
// 	db, err := sql.Open(driverName, connString)

// 	if err != nil {
// 		return false
// 	}

// 	if err := db.Ping(); err != nil {
// 		return false
// 	}

// 	return true
// }

// func CheckConnections(connections []models.Connection) (checkedConnections map[models.Connection]bool) {

// 	var wg sync.WaitGroup
// 	checkedConnections = map[models.Connection]bool{}

// 	for _, connection := range connections {
// 		wg.Add(1)
// 		checkedConnections[connection] = CheckConnectionAsync(connection, &wg)
// 	}

// 	wg.Wait()

// 	return checkedConnections
// }
