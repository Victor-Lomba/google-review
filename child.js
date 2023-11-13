const { Pool } = require("pg");
const format = require("pg-format");
const BASE_URL = "https://www.google.com/maps/rpc/listugcposts";

const INSERT_AUTHOR =
  "INSERT INTO author (id, img, name, url) VALUES ( $1, $2, $3, $4 ) ON CONFLICT (id) DO UPDATE SET img = EXCLUDED.img, name = EXCLUDED.name, url = EXCLUDED.url;";

const INSERT_REVIEW =
  "INSERT INTO GoogleReviews (id, date, rating, url, author, message, language, images, place) VALUES ($1, $2, $3, $4,	$5,	$6,	$7,	$8, $9 ) ON CONFLICT (id) DO UPDATE SET	date = EXCLUDED.date,	rating = EXCLUDED.rating, url = EXCLUDED.url,	author = EXCLUDED.author,	message = EXCLUDED.message,	language = EXCLUDED.language,	images = EXCLUDED.images;";

let placeId = "";

async function saveToDB(reviews) {
  const pool = new Pool({
    host: "database-2.cz4gx4e2nh8d.us-east-1.rds.amazonaws.com",
    port: 5432,
    database: "reviews",
    user: "postgres",
    password: process.env.password,
    max: 40,
    ssl: {
      rejectUnauthorized: false,
    },
  });

  const queriesAuthor = await reviews.map(async ({ author }) => {
    const client = await pool.connect();
    const { id, name, imgAuthor, urlAuthor } = author;

    const queryResult = client.query(INSERT_AUTHOR, [
      id,
      imgAuthor,
      name,
      urlAuthor,
    ]);

    queryResult.then(() => client.release());
    return queryResult;
  });

  // await client.query(format(INSERT_AUTHOR, values))

  await Promise.all(queriesAuthor);

  const queriesReviews = await reviews.map(
    async ({
      id,
      date,
      place,
      message,
      language,
      rating,
      reviewURL,
      author,
      images,
    }) => {
      const client = await pool.connect();

      const queryResult = client.query(INSERT_REVIEW, [
        id,
        date,
        rating,
        reviewURL,
        author.id,
        message,
        language,
        `{${images?.join(",")}}`,
        place,
      ]);

      queryResult.then(() => client.release());
      return queryResult;
    }
  );

  await Promise.all(queriesReviews);

  await pool.end();
}

function formatReview(review) {
  const reviewData = review[0];

  const timestamp = reviewData[1][2]; // in microseconds

  let [date, time] = new Date(timestamp / 1000)
    .toLocaleString("pt-BR")
    .split(",");

  date = date.split("/").reverse().join("-");
  const formattedReview = {
    id: reviewData[0],
    date: `${date} ${time}`,
    place: placeId,
    message: reviewData[2]?.[1]?.[0],
    language: reviewData[2]?.[1]?.[1],
    rating: reviewData[2]?.[0]?.[0],
    images: reviewData[2]?.[2]?.map(
      (image) => `https://lh5.googleusercontent.com/p/${image[0]}`
    ),
    reviewURL: reviewData[4][3][0],
    author: {
      id: reviewData[1][4][1][0],
      name: reviewData[1][4][0][4],
      imgAuthor: reviewData[1][4][0][3],
      urlAuthor: reviewData[1][4][0][5],
    },
  };
  return formattedReview;
}

async function fetchReviews(placeId, lastReview = "") {
  const newParams = `!1m7!1s${placeId}!3s!6m4!4m1!1e1!4m1!1e3!2m2!1i20!2s${lastReview}!3e1!5m2!1sP8pLZeXECNna1sQP8ce8sAw!7e81!8m5!1b1!2b1!3b1!5b1!7b1!11m6!1e3!2e1!3spt-BR!4sbr!6m1!1i2`;

  let completeURL = BASE_URL + "?authuser=0&hl=pt-BR&gl=br&pb=" + newParams;

  const data = await fetch(completeURL).catch((err) => console.error(err));
  if (!data) {
    return null;
  }
  const text = await data.text();
  let response = JSON.parse(text.split("\n")[1]);
  const nextReview = response[1];
  const reviews = response[2];
  const formattedReviews = reviews.map((review) => formatReview(review));
  if (nextReview) {
    const nextReviews = await fetchReviews(placeId, nextReview);
    if (!nextReviews) {
      return null;
    }
    formattedReviews.push(...nextReviews);
  }
  return formattedReviews;
}

async function handler(msg) {
  if (!msg.id) {
    process.disconnect();
  }
  let reviews;
  placeId = msg.id;

  reviews = await fetchReviews(msg.id);

  await saveToDB(reviews);

  process.send(JSON.stringify(reviews));
}

process.on("message", function child(msg) {
  handler(msg);
});
