import { Component, ElementRef, OnInit, ViewChild } from '@angular/core';
import * as am5 from '@amcharts/amcharts5';
import am5themes_Animated from "@amcharts/amcharts5/themes/Animated";
import * as am5xy from '@amcharts/amcharts5/xy';
import { BitcoinServiceService } from 'src/app/services/bitcoin-service.service';

@Component({
  selector: 'app-dashboard',
  templateUrl: './dashboard.component.html',
  styleUrls: ['./dashboard.component.scss']
})
export class DashboardComponent implements OnInit {

  @ViewChild('chartDom') chartDom: ElementRef;
  private root: am5.Root;

  private data: any[] = [];
  
  constructor(private bitcoinService: BitcoinServiceService) {

  }

  async ngOnInit() {
    this.data = await this.bitcoinService.get_all_bitcoin_data();
    this.data = this.data.map((item: any) => {
      let date = new Date(item.timestamp);
      am5.time.add(date, "day", 1);
      return {
        price: item.price,
        timestamp: date.getTime() // Convertir la chaîne en objet Date
      };
    });
    console.log(this.data);
    let root = am5.Root.new("chartdiv");
    root.setThemes([
      am5themes_Animated.new(root)
    ]);

    let chart = root.container.children.push(am5xy.XYChart.new(root, {
      panX: true,
      panY: true,
      wheelX: "panX",
      wheelY: "zoomX",
      pinchZoomX:true
    }));
    
    
    // Add cursor
    // https://www.amcharts.com/docs/v5/charts/xy-chart/cursor/
    let cursor = chart.set("cursor", am5xy.XYCursor.new(root, {
      behavior: "none"
    }));
    cursor.lineY.set("visible", false);
    
    
    // Generate random data
    // let date = this.data;
    // date.setHours(0, 0, 0, 0);
    // let value = 100;
    
    // function generateData() {
    //   // value = Math.round((Math.random() * 10 - 5) + value);
    //   // am5.time.add(date, "day", 1);

    //   this.data.forEach(element => {
    //     return {
    //       timestamp: element.timestamp,
    //       price: element.price
    //     };
    //   });

      
    // }
    
    // function generateDatas(count) {
    //   console.log(count);
    //   let data = [];
    //   for (var i = 0; i < count; ++i) {
    //     data.push(generateData());
    //   }
    //   return data;
    // }
    
    
    // Create axes
    // https://www.amcharts.com/docs/v5/charts/xy-chart/axes/
    let xAxis = chart.xAxes.push(am5xy.DateAxis.new(root, {
      maxDeviation: 0.2,
      baseInterval: {
        timeUnit: "day",
        count: 1
      },
      renderer: am5xy.AxisRendererX.new(root, {}),
      tooltip: am5.Tooltip.new(root, {})
    }));
    
    let yAxis = chart.yAxes.push(am5xy.ValueAxis.new(root, {
      renderer: am5xy.AxisRendererY.new(root, {
        pan:"zoom"
      })  
    }));
    
    
    // Add series
    // https://www.amcharts.com/docs/v5/charts/xy-chart/series/
    let series = chart.series.push(am5xy.LineSeries.new(root, {
      name: "Series",
      xAxis: xAxis,
      yAxis: yAxis,
      valueYField: "price",
      valueXField: "timestamp",
      tooltip: am5.Tooltip.new(root, {
        labelText: "{valueY}"
      })
    }));
    
    
    // Add scrollbar
    // https://www.amcharts.com/docs/v5/charts/xy-chart/scrollbars/
    chart.set("scrollbarX", am5.Scrollbar.new(root, {
      orientation: "horizontal"
    }));
    
    
    // Set data
    let data = this.data;
    console.log(data);
    series.data.setAll(data);
    
    
    // Make stuff animate on load
    // https://www.amcharts.com/docs/v5/concepts/animations/
    series.appear(1000);
    chart.appear(1000, 100);

  }

}
