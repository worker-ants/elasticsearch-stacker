export class Util {
  public static timestampToIsoString(timestamp: number) {
    return timestamp ? new Date(timestamp).toISOString() : null;
  }

  public static now() {
    return new Date().toISOString();
  }
}
