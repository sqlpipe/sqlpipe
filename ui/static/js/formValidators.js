// Example starter JavaScript for disabling form submissions if there are invalid fields
(function () {
  'use strict'

  // Fetch all the forms we want to apply custom Bootstrap validation styles to
  var forms = document.querySelectorAll('.needs-validation')

  // Loop over them and prevent submission
  Array.prototype.slice.call(forms)
    .forEach(function (form) {
      form.addEventListener('submit', function (event) {
        if (!form.checkValidity()) {
          event.preventDefault()
          event.stopPropagation()
        }
      }, false)
    })
})()

function resetStyles() {
  var elems = document.querySelectorAll(".is-invalid");

  [].forEach.call(elems, function (el) {
    el.classList.remove("is-invalid");
  });

  var success = document.getElementById("conn-success");
  success.classList.remove(...success.classList);
  success.classList.add("alert", "alert-success", "mt-4", "d-none");

  var failed = document.getElementById("conn-failed");
  failed.classList.remove(...failed.classList);
  failed.classList.add("alert", "alert-danger", "mt-4", "d-none");
}

function validateConnectionCreate() {

  resetStyles()

  let dsType = document.forms["connectionDetails"]["dsType"].value;
  let hostname = document.forms["connectionDetails"]["hostname"].value;
  let port = document.forms["connectionDetails"]["port"].value;
  let accountId = document.forms["connectionDetails"]["accountId"].value;
  let name = document.forms["connectionDetails"]["name"].value;
  let username = document.forms["connectionDetails"]["username"].value;
  let password = document.forms["connectionDetails"]["password"].value;
  let dbName = document.forms["connectionDetails"]["dbName"].value;

  isCorrect = true

  // find out if connection name is unique
  var xhr = new XMLHttpRequest();
  xhr.open("GET", `/connections/find-by-name?name=${name}`, false);
  xhr.setRequestHeader("Content-Type", 'application/json; charset=UTF-8');
  xhr.onreadystatechange = function () { // Call a function when the state changes.
    if (this.readyState === XMLHttpRequest.DONE && this.status === 200) {
      namesId = xhr.responseText
      if (namesId != "0") { // Connections name lookup returns 0 if doesn't exist
        document.getElementById("name-field").classList.add('is-invalid');
        alert("Connection name not unique. Please choose a connection name that doesn't already exist.");
        isCorrect = false;
      }
    }
  }
  xhr.send();

  if (dsType == "") {
    document.getElementById("dsType-field").classList.add('is-invalid');
    isCorrect = false;
  }

  if (name == "") {
    document.getElementById("name-field").classList.add('is-invalid');
    isCorrect = false;
  }

  if (username == "") {
    document.getElementById("username-field").classList.add('is-invalid');
    isCorrect = false;
  }
  if (password == "") {
    document.getElementById("password-field").classList.add('is-invalid');
    isCorrect = false;
  }
  if (dbName == "") {
    document.getElementById("dbName-field").classList.add('is-invalid');
    isCorrect = false;
  }

  if (dsType == "snowflake") {

    if (accountId == "") {
      document.getElementById("accountId-field").classList.add('is-invalid');
      alert("Must enter an Account ID when creating Snowflake connection.");
      isCorrect = false;
    }
    if (hostname != "") {
      document.getElementById("hostname-field").classList.add('is-invalid');
      alert("Do not enter a hostname when creating a Snowflake connection.");
      isCorrect = false;
    }
    if (port != "") {
      document.getElementById("port-field").classList.add('is-invalid');
      alert("Do not enter a port number when creating a Snowflake connection.");
      isCorrect = false;
    }
  } else if (dsType != "") {
    if (hostname == "") {
      document.getElementById("hostname-field").classList.add('is-invalid');
      alert("Must enter a hostname when creating any DB type other than Snowflake.");
      isCorrect = false;
    }
    if (port == "") {
      document.getElementById("port-field").classList.add('is-invalid');
      alert("Must enter a port number when creating any DB type other than Snowflake.");
      isCorrect = false;
    }
    if (isNaN(port)) {
      document.getElementById("port-field").classList.add('is-invalid');
      alert("The port number must be an integer");
      isCorrect = false;
    }
    if (accountId != "") {
      document.getElementById("accountId-field").classList.add('is-invalid');
      alert("Do not enter an account ID unless you are creating a Snowflake connection.");
      isCorrect = false;
    }

  }
  return isCorrect
}

function testConnection() {
  resetStyles()

  var success = document.getElementById("conn-success");
  var failed = document.getElementById("conn-failed");

  let dsType = document.forms["connectionDetails"]["dsType"].value;
  let hostname = document.forms["connectionDetails"]["hostname"].value;
  let port = parseInt(document.forms["connectionDetails"]["port"].value);
  let accountId = document.forms["connectionDetails"]["accountId"].value;
  let name = document.forms["connectionDetails"]["name"].value;
  let username = document.forms["connectionDetails"]["username"].value;
  let password = document.forms["connectionDetails"]["password"].value;
  let dbName = document.forms["connectionDetails"]["dbName"].value;

  var xhr = new XMLHttpRequest();
  xhr.open("POST", `/connections/test`, false);
  xhr.setRequestHeader("Content-Type", 'application/json; charset=UTF-8');
  xhr.onreadystatechange = function () { // Call a function when the state changes.
    if (this.readyState === XMLHttpRequest.DONE && this.status === 200) {
      if (xhr.responseText == "true") {
        success.classList.remove("d-none");
      } else {
        failed.classList.remove("d-none");
      }
    }
  }
  data = {
    "dsType": dsType,
    "hostname": hostname,
    "port": port,
    "accountId": accountId,
    "dsName": name,
    "username": username,
    "password": password,
    "dbName": dbName,
  }
  xhr.send(JSON.stringify(data));
}

function validateConnectionEdit() {

  resetStyles()

  let id = document.forms["connectionDetails"]["id"].value;
  let dsType = document.forms["connectionDetails"]["dsType"].value;
  let hostname = document.forms["connectionDetails"]["hostname"].value;
  let port = document.forms["connectionDetails"]["port"].value;
  let accountId = document.forms["connectionDetails"]["accountId"].value;
  let name = document.forms["connectionDetails"]["name"].value;
  let username = document.forms["connectionDetails"]["username"].value;
  let dbName = document.forms["connectionDetails"]["dbName"].value;

  isCorrect = true

  var xhr = new XMLHttpRequest();
  xhr.open("GET", `/connections/find-by-name?name=${name}`, false);
  xhr.setRequestHeader("Content-Type", 'application/json; charset=UTF-8');
  xhr.onreadystatechange = function () { // Call a function when the state changes.
    if (this.readyState === XMLHttpRequest.DONE && this.status === 200) {
      namesId = xhr.responseText
      if (namesId != "0") {
        if (id != namesId) {
          document.getElementById("name-field").classList.add('is-invalid');
          alert("Connection name not unique. Please choose a connection name that doesn't already exist.");
          isCorrect = false;
        }
      }
    }
  }
  xhr.send();

  if (dsType == "") {
    document.getElementById("dsType-field").classList.add('is-invalid');
    isCorrect = false;
  }

  if (name == "") {
    document.getElementById("name-field").classList.add('is-invalid');
    isCorrect = false;
  }

  if (username == "") {
    document.getElementById("username-field").classList.add('is-invalid');
    isCorrect = false;
  }

  if (dbName == "") {
    document.getElementById("dbName-field").classList.add('is-invalid');
    isCorrect = false;
  }

  if (dsType == "snowflake") {

    if (accountId == "") {
      document.getElementById("accountId-field").classList.add('is-invalid');
      alert("Must enter an Account ID when creating Snowflake connection.");
      isCorrect = false;
    }
    if (hostname != "") {
      document.getElementById("hostname-field").classList.add('is-invalid');
      alert("Do not enter a hostname when creating a Snowflake connection.");
      isCorrect = false;
    }
    if (port != "") {
      document.getElementById("port-field").classList.add('is-invalid');
      alert("Do not enter a port number when creating a Snowflake connection.");
      isCorrect = false;
    }
  } else if (dsType != "") {
    if (hostname == "") {
      document.getElementById("hostname-field").classList.add('is-invalid');
      alert("Must enter a hostname when creating any DB type other than Snowflake.");
      isCorrect = false;
    }
    if (port == "") {
      document.getElementById("port-field").classList.add('is-invalid');
      alert("Must enter a port number when creating any DB type other than Snowflake.");
      isCorrect = false;
    }
    if (isNaN(port)) {
      document.getElementById("port-field").classList.add('is-invalid');
      alert("The port number must be an integer");
      isCorrect = false;
    }
    if (accountId != "") {
      document.getElementById("accountId-field").classList.add('is-invalid');
      alert("Do not enter an account ID unless you are creating a Snowflake connection.");
      isCorrect = false;
    }

  }
  return isCorrect
}


function validateCreateTransfer() {

  resetStyles()

  let sourceId = document.forms["createTransfer"]["sourceId"].value;
  let targetId = document.forms["createTransfer"]["targetId"].value;
  let targetSchema = document.forms["createTransfer"]["targetSchema"].value;
  let targetTable = document.forms["createTransfer"]["targetTable"].value;
  let query = document.forms["createTransfer"]["query"].value;
  let overwrite = document.forms["createTransfer"]["overwrite"].value;

  isCorrect = true

  const needsSchema = ["postgresql", "mssql", "snowflake", "redshift"]

  // find out if target db needs a schema
  var xhr = new XMLHttpRequest();
  xhr.open("GET", `/connections/${targetId}`, false);
  xhr.onreadystatechange = function () { // Call a function when the state changes.
    if (this.readyState === XMLHttpRequest.DONE && this.status === 200) {
      connection = JSON.parse(xhr.response)
      if (needsSchema.includes(connection["DsType"])) {
        if (targetSchema == "") {
          document.getElementById("targetSchema-field").classList.add('is-invalid');
          alert(`If your target connection is of type ${connection["DsType"]}, you must specify a target schema.`);
          isCorrect = false;
        }
      }
    }
  }
  xhr.send();

  if (sourceId == "") {
    document.getElementById("sourceId-field").classList.add('is-invalid');
    isCorrect = false;
  }
  if (targetId == "") {
    document.getElementById("targetId-field").classList.add('is-invalid');
    isCorrect = false;
  }
  if (targetTable == "") {
    document.getElementById("targetTable-field").classList.add('is-invalid');
    isCorrect = false;
  }
  if (query == "") {
    document.getElementById("query-field").classList.add('is-invalid');
    isCorrect = false;
  }
  if (overwrite == "") {
    document.getElementById("overwrite-field").classList.add('is-invalid');
    isCorrect = false;
  }

  return isCorrect
}

