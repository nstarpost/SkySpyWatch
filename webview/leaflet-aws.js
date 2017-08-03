// var mymap = L.map('mapid').setView([51.505, -0.09], 13);
// //https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png
// //openstreetmap was really slow. using mapbox for dev at least
// L.tileLayer('https://api.tiles.mapbox.com/v4/{id}/{z}/{x}/{y}.png?access_token=pk.eyJ1Ijoic2FsdHRoZWZyaWVzIiwiYSI6ImNqMDdibXdqcTAwZXQzM3A3Ymx1aDQxeXgifQ.8LpnMxBqpCnEV7lGKg182Q', {
//   maxZoom: 18,
//   attribution: 'Map data &copy; <a href="http://openstreetmap.org">OpenStreetMap</a> contributors, ' +
//     '<a href="http://creativecommons.org/licenses/by-sa/2.0/">CC-BY-SA</a>, ' +
//     'Imagery © <a href="http://mapbox.com">Mapbox</a>',
//   id: 'mapbox.streets'
// }).addTo(mymap);

function get_aircraft_list() {
    // get list of aircraft
    var latest_timestamp;
    var aircraft_list_promise;
    var flights_to_draw = [];
    var flights = L.featureGroup();
    var icao;

    var latest_json_promise = $.getJSON('latest.json');
    latest_json_promise.done(function(data) {
        latest_timestamp = data.TimeStamp;
        //console.log(latest_timestamp);
        aircraft_list_promise = $.getJSON(latest_timestamp + '/aircraft_list_' + latest_timestamp + '.json');
        aircraft_list_promise.done(function(data) {
            //console.log("Here is the aircraft_list")
            //console.log(data.Aircraft)
            aircraft_array_length = data.Aircraft.length;
            //console.log("The length of the list is " + data.Aircraft.length)
            //update_aircraft_with_scores(data.Aircraft, latest_timestamp)
            for (var i = 0; i < aircraft_array_length; i++) {
                //console.log(data.Aircraft[i][0]);
                icao = data.Aircraft[i][0]
                $.getJSON(latest_timestamp + '/' + icao + '_' + latest_timestamp + '.json', function(data) {
                    //console.log(data['geometry'])
                    flights.addLayer(L.geoJSON(data['geometry'], {
                        style: {
                            color: 'red'
                        }
                    }).on('click', function(e) {
                        e.target.setStyle({
                            color: 'blue'
                        });
                    }).bindPopup(data['Icao'])); // .bindPopup(data['Icao'])
                    L.geoJSON(data['geometry'], {
                        style: {
                            color: 'red'
                        }
                    })
                });
            }
        });
        var overlay_flights = {
            "Flights": flights
        };
        flights.addTo(mymap);
        L.control.layers(null, overlay_flights).addTo(mymap);
    });
}

// this does not seem to work
function update_aircraft_with_scores(aircraft_list_with_scores, latest_timestamp) {
    var flights_to_draw = [];
    var flights = L.featureGroup();
    for (icao in aircraft_list_with_scores) {
        console.log(icao);
        $.getJSON(latest_timestamp + '/' + icao[0] + '_' + latest_timestamp + '.json', function(data) {
            flights.addLayer(L.geoJSON(data['geometry'], {
                style: {
                    color: 'red'
                }
            }).on('click', function(e) {
                e.target.setStyle({
                    color: 'blue'
                });
            }).bindPopup(data['Icao'])); // .bindPopup(data['Icao'])
        });
    }

    console.log(flights_to_draw)
    var overlay_flights = {
        "Flights": flights
    };
    flights.addTo(mymap);
    L.control.layers(null, overlay_flights).addTo(mymap);
}


function update_aircraft(aircraft_list) {
    var flights_to_draw = [];
    var flights = L.featureGroup();
    for (icao in aircraft_list) {
        console.log(icao);
        $.getJSON(latest_timestamp + '/' + icao + '_' + latest_timestamp + '.json', function(data) {
            flights.addLayer(L.geoJSON(data['geometry'], {
                style: {
                    color: 'red'
                }
            }).on('click', function(e) {
                e.target.setStyle({
                    color: 'blue'
                });
            }).bindPopup(data['Icao'])); // .bindPopup(data['Icao'])
        });
    }

    console.log(flights_to_draw)
    var overlay_flights = {
        "Flights": flights
    };
    flights.addTo(mymap);
    L.control.layers(null, overlay_flights).addTo(mymap);
}

var draw_flightpath2 = function(flight_json) {
    console.log(flight_json)
    var flight_linestring = flight_json['geometry']
    L.geoJSON(flight_linestring).bindPopup(flight_json["Icao"]).addTo(mymap);
}

$(document).ready(function() {
    //update_position();
    get_aircraft_list();
});
