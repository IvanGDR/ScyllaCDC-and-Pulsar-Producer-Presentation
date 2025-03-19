package com.example4;

import org.json.JSONObject;



public class JsonConvert {

    public String getSensorIdValue(String message) {
        JSONObject jsonObject = new JSONObject(message);
        String sensor_id = jsonObject.getString("sensor_id");
        return sensor_id;
    }

    public int getCoordinateValue(String message) {
        JSONObject jsonObject = new JSONObject(message);
        int coordinate = jsonObject.getInt("coordinate");
        return coordinate;
    }

    public String getStatusValue(String message) {
        JSONObject jsonObject = new JSONObject(message);
        String status = jsonObject.getString("status");
        return status;
    }
}
