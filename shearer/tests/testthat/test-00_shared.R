library(testthat)

test_that("beta_binomial_ci stays within [0, 1] and brackets its own estimate", {
  ci <- beta_binomial_ci(2, 5)
  expect_true(ci$lower >= 0 && ci$lower <= ci$estimate)
  expect_true(ci$upper <= 1 && ci$upper >= ci$estimate)
})

test_that("beta_binomial_ci gives a strictly positive interval at x = 0 and x = n (Jeffreys prior)", {
  zero <- beta_binomial_ci(0, 5)
  expect_equal(zero$lower, 0, tolerance = 1e-3)
  expect_true(zero$upper > 0)

  full <- beta_binomial_ci(5, 5)
  expect_equal(full$upper, 1, tolerance = 1e-3)
  expect_true(full$lower < 1)
})

test_that("beta_binomial_ci brackets x/n for interior cases", {
  ci <- beta_binomial_ci(30, 100)
  expect_true(ci$lower < 0.3 && ci$upper > 0.3)
})

test_that("a flat Beta(1, 1) prior widens the interval relative to Jeffreys", {
  jeffreys <- beta_binomial_ci(1, 10, prior = c(0.5, 0.5))
  flat <- beta_binomial_ci(1, 10, prior = c(1, 1))
  expect_true((flat$upper - flat$lower) > (jeffreys$upper - jeffreys$lower))
})

test_that("beta_binomial_ci agrees closely with wilson_ci at moderate n", {
  bb <- beta_binomial_ci(40, 100)
  w <- wilson_ci(40, 100)
  expect_equal(bb$estimate, w$estimate, tolerance = 0.02)
  expect_equal(bb$lower, w$lower, tolerance = 0.03)
  expect_equal(bb$upper, w$upper, tolerance = 0.03)
})

test_that("beta_binomial_ci errors informatively when n is 0", {
  expect_error(beta_binomial_ci(0, 0), "n must be > 0")
})
