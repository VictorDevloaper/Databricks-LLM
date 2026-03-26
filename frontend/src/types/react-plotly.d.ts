declare module 'react-plotly.js' {
  import * as React from 'react';

  interface PlotParams {
    data: any[];
    layout?: any;
    frames?: any[];
    config?: any;
    onSelected?: (event: any) => void;
    className?: string;
    style?: React.CSSProperties;
    useResizeHandler?: boolean;
    onInitialized?: (figure: any, graphDiv: any) => void;
    onUpdate?: (figure: any, graphDiv: any) => void;
    onPurge?: (figure: any, graphDiv: any) => void;
    onError?: (err: any) => void;
  }

  export default class Plot extends React.Component<PlotParams> {}
}
