//Some airlines formed alliance, the alliance collection contains 3 alliances. The question is to find the alliance with the highest number of unique carriers flying between JFK and LHR airports. The $lookup is use to connect the air_routes collection to the air_alliances collection

db.air_routes.aggregate([
  {
    $match: { $and: [{ src_airport: /JFK|LHR/ }, { dst_airport: /LHR|JFK/ }] }
  },
  { $group: { _id: "$airline.name" } },
  {
    $lookup: {
      from: "air_alliances",
      foreignField: "airlines",
      localField: "_id",
      as: "alliance"
    }
  },
  { $unwind: "$alliance" },
  { $group: { _id: "$alliance.name", count: { $sum: 1 } } }
]);

//The question is to find the number of movies with an imdb.rating of at least 7 and the genre does not contain crime of horror. The rating should be either PG or G and it should be available in both English and Japanese
db.movies.aggregate([
  {
    $match: {
      "imdb.rating": { $gte: 7 },
      genres: { $nin: ["Crime", "Horror"] },
      rated: { $in: ["PG", "G"] },
      languages: { $all: ["English", "Japanese"] }
    }
  }
]);

// The question is to find the title of the 25th movie from the result obtained when the movies release in the USA with a tomatoes.viewer.rating greater than 3 are evaluated. I was required to compute a new field called num_favs that represents how many favorites appear in the cast field of the movie then to sort the results by num_favs, tomatoes.viewer.rating, and title, all in descending order.

db.movies
  .aggregate(
    [
      {
        $match: {
          $and: [
            { "tomatoes.viewer.rating": { $gte: 3 } },
            { cast: { $exists: true } },
            { countries: "USA" }
          ]
        }
      },
      { $addFields: { cast: { $setIntersection: ["$cast", favorites] } } },
      { $addFields: { num_favs: { $size: "$cast" } } },
      { $sort: { num_favs: -1, "tomatoes.viewer.rating": -1, title: -1 } },
      { $skip: 24 },
      { $limit: 1 },
      { $project: { title: 1 } }
    ],
    { allowDiskUse: true }
  )
  .pretty();

// Calculate an average rating for each movie in our collection where English is an available language, the minimum imdb.rating is at least 1, the minimum imdb.votes is at least 1, and it was released in 1990 or after. You'll be required to rescale (or normalize) imdb.votes. The formula to rescale imdb.votes and calculate normalized_rating is included as a handout. What film has the lowest normalized_rating ?

db.movies.aggregate([
  {
    $match: {
      $and: [
        { languages: "English" },
        { "imdb.rating": { $gte: 1 } },
        { year: { $gte: 1990 } },
        { "imdb.votes": { $exists: true } }
      ]
    }
  },
  {
    $addFields: {
      normalized_rating: {
        $avg: [
          {
            $add: [
              1,
              {
                $multiply: [
                  9,
                  {
                    $divide: [
                      {
                        $subtract: ["$imdb.votes", 5]
                      },
                      { $subtract: [1521105, 5] }
                    ]
                  }
                ]
              }
            ]
          },
          "$imdb.rating"
        ]
      }
    }
  },
  {
    $match: {
      normalized_rating: { $gte: 1 }
    }
  },
  {
    $sort: { normalized_rating: 1 }
  },
  { $limit: 1 },
  { $project: { title: 1 } }
]);

//This is the formular for calculating normalized scaling, this formula is provided with the question
min + (max - min) * ((x - x_min) / (x_max - x_min));
scaled_votes = 1 + 9 * ((x - x_min) / (x_max - x_min));
x_max = 1521105;
x_min = 5;
min = 1;
max = 10;
x = imdb.votes;

// within a pipeline, it should look something like the following
/*
  {
    $add: [
      1,
      {
        $multiply: [
          9,
          {
            $divide: [
              { $subtract: [<x>, <x_min>] },
              { $subtract: [<x_max>, <x_min>] }
            ]
          }
        ]
      }
    ]
  }
*/

// given we have the numbers, this is how to calculated normalized_rating
// yes, you can use $avg in $project and $addFields!
normalized_rating = average(scaled_votes, imdb.rating);

//The question is to find all films that won at least 1 Oscar, and calculate the standard deviation, highest, lowest, and average imdb.rating using the sample standard deviation expression($stdDevSamp).

db.movies.aggregate([
  {
    $match: {
      $and: [
        {
          awards: {
            $exists: true
          }
        },
        {
          $or: [
            {
              "awards.text": {
                $regex: /^Won .* Oscar/gi
              }
            },
            {
              awards: {
                $regex: /^Won .* Oscar/gi
              }
            }
          ]
        }
      ]
    }
  },
  {
    $group: {
      _id: 0,
      highest_rating: {
        $max: "$imdb.rating"
      },
      lowest_rating: {
        $min: "$imdb.rating"
      },
      avg_rating: {
        $avg: "$imdb.rating"
      },
      deviation: {
        $stdDevSamp: "$imdb.rating"
      }
    }
  }
]);

//The question is to calculate how many movies every cast member has been in and get an average imdb.rating for each cast member. What is the name, number of movies, and average rating(truncated to one decimal) for the cast member that has been in the most number of movies with English as an available language ? Provide the input in the following order and format { "_id": "First Last", "numFilms": 1, "average": 1.1 }?

db.movies.aggregate(
  [
    {
      $match: {
        languages: "English"
      }
    },
    {
      $project: { cast: 1, imdb: 1, languages: 1, title: 1 }
    },
    { $unwind: "$cast" },
    {
      $group: {
        _id: "$cast",
        movies: {
          $push: { imdb: "$imdb", languages: "$languages", title: "$title" }
        },
        numFilms: {
          $sum: 1
        }
      }
    },
    {
      $project: {
        _id: "$_id",
        numFilms: "$numFilms",
        average: {
          $avg: "$movies.imdb.rating"
        }
      }
    },
    {
      $sort: {
        numFilms: -1
      }
    }
  ],
  { allowDiskUse: true }
);

//The question is to find which alliance from air_alliances flies the most routes with either a Boeing 747 or an Airbus A380 (abbreviated 747 and 380 in air_routes)?

db.air_routes.aggregate([
  {
    $match: {
      airplane: /747|380/
    }
  },
  {
    $lookup: {
      from: "air_alliances",
      localField: "airline.name",
      foreignField: "airlines",
      as: "airlines"
    }
  },
  {
    $unwind: "$airlines"
  },
  {
    $group: {
      _id: { name: "$airlines.name" },
      num: {
        $sum: 1
      }
    }
  },
  {
    $sort: { count: -1 }
  }
]);

//Find the list of all possible distinct destinations, with at most one layover, departing from the base airports of airlines that make part of the "OneWorld" alliance. The airlines should be national carriers from Germany, Spain or Canada only. Include both the destination and which airline services that location

db.air_alliances.aggregate([
  {
    $match: {
      name: /OneWorld/
    }
  },
  {
    $graphLookup: {
      from: "air_airlines",
      startWith: "$airlines",
      connectFromField: "airlines",
      connectToField: "name",
      as: "airlines",
      restrictSearchWithMatch: {
        country: {
          $in: ["Germany", "Spain", "Canada"]
        }
      }
    }
  },
  {
    $graphLookup: {
      from: "air_routes",
      startWith: "$airlines.base",
      connectFromField: "dst_airport",
      connectToField: "src_airport",
      maxDepth: 1,
      as: "connections"
    }
  },
  {
    $project: {
      validAirlines: "$airlines.name",
      "connections.dst_airport": 1,
      "connections.airline.name": 1
    }
  },
  { $unwind: "$connections" },
  {
    $project: {
      isValid: { $in: ["$connections.airline.name", "$validAirlines"] },
      "connections.dst_airport": 1
    }
  },
  { $match: { isValid: true } },
  { $group: { _id: "$connections.dst_airport" } }
]);

//How many movies are in both the top ten highest rated movies according to the imdb.rating and the metacritic fields? We should get these results with exactly one access to the database.

db.movies.aggregate(
  [
    {
      $match: {
        "imdb.rating": {
          $gte: 0
        },
        metacritic: { $gte: 0 }
      }
    },
    {
      $facet: {
        rating: [
          {
            $sort: { "imdb.rating": -1 }
          },
          {
            $limit: 10
          },
          {
            $project: {
              title: 1,
              imdb: 1
            }
          }
        ],
        critic: [
          {
            $sort: {
              metacritic: -1
            }
          },
          {
            $limit: 10
          },
          {
            $project: {
              title: 1,
              metacritic: 1
            }
          }
        ]
      }
    },
    {
      $project: {
        inter: {
          $setIntersection: ["$rating.title", "$critic.title"]
        }
      }
    }
  ],
  { allowDiskUse: true }
);
