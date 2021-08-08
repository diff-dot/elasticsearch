export interface BuildQueryParams {
  [key: string]: string | number | string[] | number[] | undefined;
  source?: string | string[] | undefined;
  index?: string;
}
