# Databricks notebook source
# MAGIC %md
# MAGIC #### Airline Loyalty - What Affect's Customer's Loyalty? 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### About the Dataset
# MAGIC 
# MAGIC ##### This data has membership, purchase, spend, travel info about airline loyalty programme customers
# MAGIC 
# MAGIC 1. ID
# MAGIC 2. Gender (0- Male, 1 - Female)
# MAGIC 3. Age (0 - Below 20, 1 - From 20 to 29, 2 - From 30 to 39, 3 - 40 and above)
# MAGIC 4. Status (0- Student, 1 - Self Employed, 2 - Employed, 3 - Homemaker )
# MAGIC 5. Income (0- Less than 25k AUD, 1 - Between 25k and 50k, 2 - Between 50k and 100k, 3 - Between 100k and 150k, 4 - More than 150k )
# MAGIC 6. NoofFlightsPermonth
# MAGIC 7. Class - (0 - Economy, 1 - Business, 2 - Premium Economy, 3 - First Class, 4 - Chartered )
# MAGIC 8. FlightDuration (0 -  < 30 mins, 1 - 30 to 1h, 2 - 1h to 2h, 3 - 2h to 3h, 4 - More than 3h)
# MAGIC 9. Location - (0 - Suburbs, 1 - Remote, 2 - Inner City)
# MAGIC 10. Membership Card
# MAGIC 11. IsFuelUser
# MAGIC 12. IsOnlineSpend
# MAGIC 13. IsInstoreSpend
# MAGIC 14. IsLoungeUser
# MAGIC 15. ItemPurchaseOthers
# MAGIC 16. SpendPurchase (0 -Zero, 1 - Less than AUD200, 2 - AUD 200 to 400, 3 - More than AUD400)
# MAGIC 17. ProductRate - (Scaled from 1-5 -> 1 - Very Bad, 5 - Excellent)
# MAGIC 18. PriceRate - (Scaled from 1-5 -> 1 - Very Bad, 5 - Excellent)
# MAGIC 19. PromoRate - (Scaled from 1-5 -> 1 - Very Bad, 5 - Excellent)
# MAGIC 20. ServiceRate - (Scaled from 1-5 -> 1 - Very Bad, 5 - Excellent)
# MAGIC 21. PromoMethodApp - (0 - Yes, 1 - No)
# MAGIC 22. PromoMethodSoc - (0 - Yes, 1 - No)
# MAGIC 23. PromoMethodSoc - (0 - Yes, 1 - No)
# MAGIC 24. PromoMethodEmail - (0 - Yes, 1 - No)
# MAGIC 25. PromoMethodFriend - (0 - Yes, 1 - No)
# MAGIC 26. PromoMethodDisplay - (0 - Yes, 1 - No)
# MAGIC 27. PromoMethodBillBoard - (0 - Yes, 1 - No)
# MAGIC 28. PromoMethodOthers - (0 - Yes, 1 - No)
# MAGIC 29. Loyal - (0 - Yes, 1 - No)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Install corrplot, biocmanager, NMF, psych, ggthemes, GPArotation from CRAN

# COMMAND ----------

install.packages("psych")
library(tidyverse)
library(lattice)
library(caret)
library(corrplot)
library(psych)
BiocManager::install("Biobase")
#library(NMF)
library(grid)
library(ggthemes)
library(gridExtra)
library(knitr)

data <- read.csv("/dbfs/mnt/deepaksekaradls/Airlines_Data/Loyalty/VALoyalty.csv")

data <- na.omit(data)

data$loyal <-ifelse(data$loyal==0,1,0) # recoding loyal/disloyal customers
data <- data[,-nearZeroVar(data)]  # removing variables with little variance

# COMMAND ----------

head(data)

# COMMAND ----------

# MAGIC %md
# MAGIC #### What Contributes to Positive Perception of Airline's Brand Image

# COMMAND ----------

o <- omega(data[,10:15], nfactors=3, plot=F)
omega.diagram(o, digits=2, main="Attributes of Brand Image")

# COMMAND ----------

# MAGIC %md
# MAGIC #### What affect's Customer's Loyalty? 
# MAGIC ##### Logistic Regression

# COMMAND ----------

install.packages('e1071', dependencies=TRUE)

glm_fit <- glm(loyal ~ ., data, family="binomial")
p <- predict(glm_fit, newdata = data, type = "response")
pred <- ifelse(p>0.5,1,0)
tab <- table(Predicted=pred, Actual=data$loyal)
confusionMatrix(tab)
c <- as.data.frame(glm_fit$coefficients)
c$name <- rownames(c)
colnames(c)[1] <- "coef"
c$odds <- exp(c$coef)

# COMMAND ----------

options(repr.plot.width=13, repr.plot.height=13)

c %>% filter(name!="(Intercept)" & name!="predict") %>%  ggplot(aes(reorder(name,odds),odds)) + 
    geom_bar(stat = "identity") + 
    geom_label(aes(label=round(odds,2)),size=8) +
    coord_flip() +
    theme_fivethirtyeight() +
    theme(axis.text=element_text(size=12), plot.subtitle = element_text(size=12), plot.caption = element_text(size=12), panel.grid.major = element_blank(), panel.grid.minor = element_blank()) +
    geom_hline(yintercept = 1, color="red", linetype="dashed") +
    labs(title = 'Factors Affecting Customers Loyalty (Odds Ratio)', subtitle = "factors with odds ratio greater than 1 positively affect loyalty", caption = "*** interpretation: a 1 unit increase in spending category increases the odds of loyalty by a factor of 7.9 ***")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Customer's Segmentation Using Matrix Factorization

# COMMAND ----------

install.packages('NMF', dependencies=TRUE)
library(NMF)

# COMMAND ----------

fit <- nmf(data[,-1], 5, "lee", seed=14) 
options(repr.plot.width = 10000, repr.plot.height = 500, repr.plot.res = 144, repr.plot.pointsize = 10)
ht <- grid.grabExpr(coefmap(fit, color = "YlOrRd:50", scale = "c1", main="Decomposing Survey to 5 Groups", fontsize=5, cexCol=2/1, cexRow=2/1, tracks=NA))
#grid.newpage()
pushViewport(viewport(angle = 90))
grid.draw(ht)
popViewport()

# COMMAND ----------

# clustering
options(repr.plot.width=10, repr.plot.height=10)
w <- basis(fit)
type <- max.col(w) 
data$cluster <- type

# COMMAND ----------

cluster <- data %>% group_by(cluster) %>% summarise(n=n(), STATUS=median(status), AGE=mean(age), SPEND=median(spendPurchase), PRICE=round(mean(priceRate),1),
                                                    SERVICE=round(mean(serviceRate),1), LOYAL=round(mean(loyal),1))

# recoding categorical variables for better readability
cluster[,'AGE'] <- c("20 to 29","20 to 29","30 to 29","30 to 39","Above 39")
cluster[,'STATUS'] <- c("Employed","Self-Employed","Employed","Self-Employed","Homemaker")
cluster[,'SPEND'] <- c("less than 200","less than 200","200 to 400","200 to 400","More than 400")

kable(cluster, caption="Groups of Customers After Clustering")
