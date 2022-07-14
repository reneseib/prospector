function run(Lt) {
  var La = "d93"
    .split("")
    .map(function (LR) {
      return LR.charCodeAt();
    })
    .reduce(function (Lu, pD) {
      return Lu + pD;
    });
  return parseInt(
    (
      (((La + parseInt("d93", 36)) % 1000) + 1) *
        (Math.floor(new Date() / 1000) - Lt) *
        10 +
      Math.round(Math.random() * 8) +
      1
    )
      .toString()
      .split("")
      .reverse()
      .join(""),
    10
  ).toString(36);
}
