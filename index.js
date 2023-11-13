const { fork } = require("child_process");
const CHILD_PATH = "./child.js";
const pg = require("pg");
const format = require("pg-format");

async function handler(event) {
  let reviews = [];

  const promises = event.ids.map((id) => {
    return new Promise((resolve) => {
      const child = fork(CHILD_PATH);
      child.on("message", (msg) => {
        resolve(JSON.parse(msg));
        child.disconnect();
      });
      child.send({ id: id });
    });
  });

  reviews = await Promise.all(promises);

  return {
    statusCode: 200,
    body: JSON.stringify(reviews),
  };
}
module.exports = {
  handler,
};
