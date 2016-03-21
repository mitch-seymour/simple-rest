<?php

require('src/SimpleREST.php');

// get a SimpleREST client
$rest = new Simpler\DataStore\SimpleREST();

// set the request headers and response behavior
$rest->json()->autoConvert();

// get a request bin
$response = $rest->location('http://requestb.in/api/v1/bins')->post(array('private' => false));


if (!isset($response['name']) || $rest->responseCode() !== 200){
    
    // could not retrieve a request bin
    exit('Unable to retrieve request bin');
}

$requestBin = 'http://requestb.in/' . $response['name'];

// execute some requests so we can see them in the request bin
$rest->location($requestBin)->get(array('hello' => 'world'));
$rest->location($requestBin)->post(array('hello' => 'world'));


echo 'Inspect your requests here: ' . PHP_EOL . $requestBin . '?inspect';