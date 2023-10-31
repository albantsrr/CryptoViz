import { Injectable } from '@angular/core';
import axios from 'axios';

@Injectable({
  providedIn: 'root'
})
export class BitcoinServiceService {

  private endpoint_bitcoin = "http://localhost:5000/api/bitcoin";
  private axiosInstanceBaseBitcoin = axios.create({
    headers: {
      'Access-Control-Allow-Origin': '*', // Replace with your desired origin
    },
  });

  constructor() { }

  async get_all_bitcoin_data() {
    try {
      const bitcoin_data = await this.axiosInstanceBaseBitcoin.get(`${this.endpoint_bitcoin}/get_bitcoin_data_history`, {});
      return bitcoin_data.data;
    } catch (error) {
      console.error(error);
    }
  }

}
