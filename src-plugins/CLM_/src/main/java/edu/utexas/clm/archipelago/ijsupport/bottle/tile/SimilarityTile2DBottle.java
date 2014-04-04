package edu.utexas.clm.archipelago.ijsupport.bottle.tile;

import edu.utexas.clm.archipelago.network.MessageXC;
import edu.utexas.clm.archipelago.network.translation.Bottle;
import ini.trakem2.display.Patch;
import mpicbg.models.SimilarityModel2D;
import mpicbg.trakem2.align.AbstractAffineTile2D;
import mpicbg.trakem2.align.SimilarityTile2D;

import java.io.IOException;

/**
 *
 */
public class SimilarityTile2DBottle implements Bottle<AbstractAffineTile2D<?>>
{
    private final SimilarityModel2D model;
    private final Patch patch;

    public SimilarityTile2DBottle(final SimilarityTile2D tile)
    {
        model = tile.getModel();
        patch = tile.getPatch();
    }

    public AbstractAffineTile2D<?> unBottle(final MessageXC xc) throws IOException
    {
        return new SimilarityTile2D(model, patch);
    }
}
