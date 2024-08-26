db = db.getSiblingDB("swatch_devs");

db.createUser({
    user: "iot_user",
    pwd: "42",
    roles: [
      {
        role: 'readWrite',
        db: 'swatch_devs'
      },
    ],
  });

db.createCollection("users");
var users = JSON.parse(fs.readFileSync("./shared_data/users.json"));
db.users.insertMany(users);
delete(users);

db.createCollection("heart_rates");
var heart_rates = JSON.parse(fs.readFileSync("./shared_data/heart_rates.json"));
db.heart_rates.insertMany(heart_rates);
delete(heart_rates);

db.createCollection("sleeps");
var sleeps = JSON.parse(fs.readFileSync("./shared_data/sleeps.json"));
db.sleeps.insertMany(sleeps);
delete(sleeps);

db.createCollection("steps");
var steps = JSON.parse(fs.readFileSync("./shared_data/steps.json"));
db.steps.insertMany(steps);
delete(steps);
