const SERVER_URL = 'http://YOUR_SERVER_IP:1080/push';

function doPost(e) {
  const gasKey = e.parameter.key;
  const clientID = e.parameter.id;
  
  const payload = (e && e.postData && e.postData.getBytes()) || [];

  const options = {
    method: 'post',
    contentType: 'application/octet-stream',
    headers: {
      'X-Gas-Key': gasKey,
      'X-Client-ID': clientID
    },
    payload: payload,
    muteHttpExceptions: true
  };

  try {
    const resp = UrlFetchApp.fetch(SERVER_URL, options);
    return ContentService.createTextOutput("OK")
      .setMimeType(ContentService.MimeType.TEXT);
  } catch (err) {
    return ContentService.createTextOutput("Error: " + err.toString())
      .setMimeType(ContentService.MimeType.TEXT);
  }
}

function doGet(e) {
  const status = {
    status: "Zephyr is Active"
  };
  
  return ContentService.createTextOutput(JSON.stringify(status))
    .setMimeType(ContentService.MimeType.JSON);
}